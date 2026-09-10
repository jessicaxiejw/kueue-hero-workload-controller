// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

// Package drainctrl is the drain controller: design doc phases 0-2. It
// watches Workloads for stuck heroes and, one drain at a time per cluster,
// taints the selected domains and cycles the victims out through Kueue.
package drainctrl

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/drain"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/hero"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/metrics"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/selection"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/snapshot"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/taint"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/victims"
)

// DeactivatedForAnnotation aliases the shared victim marker; see
// pkg/victims for the suspend/reactivate contract.
const DeactivatedForAnnotation = victims.DeactivatedForAnnotation

// Event reasons emitted on the hero Workload. Keep in sync with
// docs/submitting-hero-workloads.md's troubleshooting table.
const (
	EventHeroExceedsQuota        = "HeroExceedsQuota"
	EventDrainQueued             = "DrainQueued"
	EventNoFeasibleDomains       = "NoFeasibleDomains"
	EventAllFeasibleHeroOccupied = "AllFeasibleDomainsHeroOccupied"
	EventDrainPlannedDryRun      = "DrainPlannedDryRun"
	EventDomainsTainted          = "DomainsTainted"
	EventTopologyLevelUnknown    = "TopologyLevelUnknown"
	EventVictimsSuspended        = "VictimsSuspended"
	EventDrainAborted            = "DrainAborted"
)

// Event reasons emitted on victim Workloads (reactivation's event lives in
// pkg/victims alongside the shared reactivation logic).
const (
	EventSuspendedForHero   = "SuspendedForHeroDrain"
	EventReactivatedByDrain = victims.EventReactivated
)

const (
	requeueWhileQueued   = 30 * time.Second
	requeueNoFeasible    = time.Minute
	requeueVictimCycling = 15 * time.Second
)

// Reconciler drives phases 0-2 for one stuck hero Workload at a time.
type Reconciler struct {
	client.Client
	Recorder record.EventRecorder
	Cfg      *config.Config
	// Clock injectable for tests; defaults to time.Now via SetupWithManager.
	Now func() time.Time
	// Nudge, when set, delivers a signal each time the janitor tears a
	// drain down: every stuck workload is re-enqueued immediately, so
	// queued heroes start their drains without waiting for the poll.
	Nudge <-chan event.GenericEvent
}

// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=clusterqueues;localqueues;resourceflavors;topologies,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is idempotent and stateless: every call re-derives hero-ness,
// stuckness, drain-in-flight (from node taints), and victim state (from
// annotations) before acting.
//
// Log verbosity convention: V(2) is the full debug trace of the hero drain
// pipeline — every decision taken for a workload that IS a hero. V(3) adds
// the workloads this controller walks past (victims, deactivated
// workloads, non-heroes), which is the level to reach for when a workload
// you expected to be drained for produces no V(2) output at all.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("hero", req.NamespacedName)

	wl := &kueue.Workload{}
	if err := r.Get(ctx, req.NamespacedName, wl); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// A workload with the deactivated-for annotation is a victim we
	// suspended: finish its suspend/reactivate cycle and never run the
	// hero drain pipeline on it.
	if ownerRef, ok := wl.Annotations[DeactivatedForAnnotation]; ok {
		log.V(3).Info("workload is a victim of a drain, not a hero", "drainOwner", ownerRef,
			"active", wl.Spec.Active == nil || *wl.Spec.Active)
		return r.reconcileVictimEvent(ctx, wl, ownerRef)
	}

	// A deactivated workload (spec.active=false) is never drained for:
	// kueue will not admit it however empty the domain gets, and
	// deactivating the hero is the documented operator stop switch for a
	// timeout eviction loop. Its stuck condition survives deactivation, so
	// without this gate the pipeline would keep draining — and if it holds
	// taints, fight the janitor's abandoned-path teardown by re-tainting
	// node by node (observed in e2e as a taint/untaint storm). Cleanup of
	// any in-flight drain belongs to the janitor alone.
	if wl.Spec.Active != nil && !*wl.Spec.Active {
		log.V(3).Info("workload is deactivated; never drained for")
		return ctrl.Result{}, nil
	}

	cq, err := r.clusterQueueFor(ctx, wl)
	if err != nil {
		return ctrl.Result{}, err
	}
	if isHero, reason := hero.IsHero(wl, cq, r.Cfg); !isHero {
		return r.handleNonHero(ctx, wl, reason)
	}

	log.V(2).Info("hero identified", "hero", req.NamespacedName, "cq", cq.Name)

	// Check the nodes for a drain this hero already started (the taints are
	// the record). Even when the hero no longer needs help — usually because
	// the drain worked and kueue admitted it — we may still owe cleanup:
	// victims we suspended could still be waiting to be turned back on.
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes); err != nil {
		return ctrl.Result{}, err
	}
	drains := taint.FindDrains(nodes.Items, r.Cfg.TaintKey)
	own := drains[req.NamespacedName]
	metrics.DrainsInFlight.Set(float64(len(drains)))
	log.V(2).Info("drain state read from node taints", "nodes", len(nodes.Items),
		"drainsInFlight", len(drains), "ownDrain", own != nil, "ownNodes", drainNodeCount(own))

	if !hero.IsStuckTASNoFit(wl, r.Cfg.StuckDetection) {
		if own != nil {
			// Admitted (or otherwise unstuck) with our taints still up:
			// untainting is the janitor's job; victims may still cycle.
			log.V(2).Info("hero no longer stuck with own drain up; cycling victims",
				"admitted", wl.Status.Admission != nil, "nodes", len(own.Nodes))
			return r.cycleVictims(ctx, wl, own.Nodes)
		}
		log.V(2).Info("hero is not stuck on a TAS no-fit; nothing to do")
		return ctrl.Result{}, nil
	}
	heroGPU := hero.GPURequest(wl, r.Cfg.GPUResourceName)
	log.V(2).Info("hero is stuck on a TAS no-fit", "priority", hero.Priority(wl),
		"podCount", hero.PodCount(wl), "gpuRequest", heroGPU.String())

	// Phase 0: quota precondition.
	if res, err := r.refuseOverQuota(ctx, wl, cq, own); res != nil {
		return *res, err
	}

	// Phase 0: serialize — one hero drain at a time per cluster, across
	// all ClusterQueues. Only drain taints attributable to a hero count
	// here (other node taints, and drain-key taints this controller
	// cannot attribute, never block): wait until no node carries another
	// hero's drain taint.
	if blocking := drain.BlockingDrain(drains, req.NamespacedName); blocking != nil {
		r.reportDrainQueued(ctx, wl, blocking)
		return ctrl.Result{RequeueAfter: requeueWhileQueued}, nil
	}

	// Phase 0: among all stuck heroes cluster-wide, only the winner
	// starts a drain.
	allWorkloads := &kueue.WorkloadList{}
	if err := r.List(ctx, allWorkloads); err != nil {
		return ctrl.Result{}, err
	}
	stuck, err := r.stuckHeroes(ctx, allWorkloads.Items)
	if err != nil {
		return ctrl.Result{}, err
	}
	log.V(2).Info("stuck heroes cluster-wide", "count", len(stuck),
		"workloads", len(allWorkloads.Items))
	// One drain runs at a time, and the turn goes to whichever stuck hero
	// is first in line (NextHero: priority, then age). There is no central
	// queue: every stuck hero's reconcile computes the same ordering
	// independently and simply exits if it is not the winner.
	//
	// The one exception is a hero already mid-drain: it finishes its drain
	// even if a higher-priority hero shows up. Yielding would not help the
	// newcomer anyway; the owner's taints would still be up, blocking the
	// newcomer, and now nobody would finish. So drains run to completion.
	if own == nil {
		next := drain.NextHero(stuck)
		isNextInLine := next != nil && next.Namespace == wl.Namespace && next.Name == wl.Name
		if !isNextInLine {
			log.V(1).Info("another stuck hero is ahead in line", "next", workloadKey(next))
			return ctrl.Result{RequeueAfter: requeueWhileQueued}, nil
		}
		log.V(2).Info("hero is next in line; starting drain")
	} else {
		// Resuming: idempotent re-selection below re-taints any node
		// missed before a crash; a changed decision's stale taints are
		// cleaned up by the janitor at drain end.
		log.V(1).Info("resuming own drain", "nodes", len(own.Nodes))
	}

	// Phase 1: snapshot and selection.
	level, groupingLevel, res := r.drainLevel(ctx, wl, cq)
	if res != nil {
		return *res, nil
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods); err != nil {
		return ctrl.Result{}, err
	}
	otherHeroes, err := r.otherAdmittedHeroes(ctx, allWorkloads.Items, req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, err
	}
	log.V(2).Info("building snapshot", "level", level, "parentLevel", parentLevel,
		"nodes", len(nodes.Items), "pods", len(pods.Items), "otherAdmittedHeroes", len(otherHeroes))
	snap := snapshot.Build(snapshot.Input{
		Level:       level,
		GroupLevel:  groupingLevel,
		Nodes:       nodes.Items,
		Pods:        pods.Items,
		OtherHeroes: otherHeroes,
		Self:        req.NamespacedName,
		Cfg:         r.Cfg,
	})

	workloadMap := map[types.NamespacedName]*kueue.Workload{}
	for i := range allWorkloads.Items {
		w := &allWorkloads.Items[i]
		workloadMap[types.NamespacedName{Namespace: w.Namespace, Name: w.Name}] = w
	}

	heroSpec := selection.HeroSpec{
		Key:          req.NamespacedName,
		Priority:     hero.Priority(wl),
		PodCount:     hero.PodCount(wl),
		ClusterQueue: cq.Name,
		Demand:       hero.Demand(wl, r.Cfg.GPUResourceName),
	}
	log.V(2).Info("hero demand for selection", "priority", heroSpec.Priority,
		"podCount", heroSpec.PodCount, "cq", heroSpec.ClusterQueue,
		"demand", formatDemand(heroSpec.Demand), "domains", len(snap.Domains))
	for id, d := range snap.Domains {
		log.V(2).Info("domain snapshot", "domain", id,
			"nodes", len(d.Nodes), "allocatableGPU", d.AllocatableGPU.String(),
			"nonReclaimableGPU", d.NonReclaimableGPU.String(),
			"victims", len(d.Victims), "hasOtherHero", d.HasOtherHero)
	}
	drainPlan, infeasibleReason := selection.SelectDomains(snap, heroSpec, workloadMap, r.Cfg, r.Now())
	metrics.SelectionOutcomes.WithLabelValues(selectionOutcomeLabel(drainPlan, infeasibleReason)).Inc()
	if drainPlan == nil {
		log.V(2).Info("no drain plan", "reason", infeasibleReason, "level", level,
			"ownDrain", own != nil)
		if own != nil {
			// The world changed mid-drain and the hero can no longer fit
			// even after full eviction: abort. Suspending more victims
			// would be pointless harm, and holding the taints pins a
			// domain that cannot serve the hero.
			return r.abortDrain(ctx, wl, own, string(infeasibleReason))
		}
		eventType, eventReason, message, result := infeasibleResponse(infeasibleReason, level)
		r.Recorder.Event(wl, eventType, eventReason, message)
		return result, nil
	}

	log.V(2).Info("drain plan selected", "domains", drainPlan.DomainIDs,
		"nodes", drainPlan.Nodes, "victims", len(drainPlan.Victims),
		"cost", drainPlan.TotalCost, "dryRun", r.Cfg.DryRun)

	if r.Cfg.DryRun {
		r.Recorder.Eventf(wl, corev1.EventTypeNormal, EventDrainPlannedDryRun,
			"dry-run: would taint domains %v (%d nodes, %d victim workloads, cost %.3f)",
			drainPlan.DomainIDs, len(drainPlan.Nodes), len(drainPlan.Victims), drainPlan.TotalCost)
		return ctrl.Result{}, nil
	}

	// Phase 2a: taint every node of every selected domain BEFORE evicting
	// anyone — evicted victims requeue immediately, and an untainted
	// selected domain is exactly where Kueue would put them back.
	for _, nodeName := range drainPlan.Nodes {
		if err := taint.EnsureTaint(ctx, r.Client, nodeName, r.Cfg.TaintKey, req.NamespacedName, cq.Name, r.Now()); err != nil {
			return ctrl.Result{}, fmt.Errorf("tainting %s: %w", nodeName, err)
		}
		log.V(2).Info("node tainted for drain", "node", nodeName)
	}
	if own == nil { // first taint application for this hero = drain started
		metrics.DrainsStarted.Inc()
	}
	log.Info("domains tainted for drain", "domains", drainPlan.DomainIDs,
		"nodes", len(drainPlan.Nodes), "victims", len(drainPlan.Victims), "cost", drainPlan.TotalCost)
	r.Recorder.Eventf(wl, corev1.EventTypeNormal, EventDomainsTainted,
		"tainted domains %v (%d nodes) for drain; evicting %d victim workloads",
		drainPlan.DomainIDs, len(drainPlan.Nodes), len(drainPlan.Victims))

	// Phase 2b: cycle victims.
	return r.cycleVictims(ctx, wl, drainPlan.Nodes)
}

// cycleVictims is the stateless suspend/unsuspend loop over the tainted
// nodes: victims still admitted there get spec.active=false plus our
// ownership annotation; victims we deactivated that Kueue has since
// evicted get spec.active=true back (the taint keeps them out of the
// drained domain when they requeue).
func (r *Reconciler) cycleVictims(ctx context.Context, heroWL *kueue.Workload, taintedNodes []string) (ctrl.Result, error) {
	ownerRef := heroWL.Namespace + "/" + heroWL.Name
	tainted := map[string]bool{}
	for _, n := range taintedNodes {
		tainted[n] = true
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods); err != nil {
		return ctrl.Result{}, err
	}
	pending := 0
	suspended := 0
	seen := map[types.NamespacedName]bool{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !tainted[pod.Spec.NodeName] {
			continue
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		wlName, ok := pod.Annotations[kueue.WorkloadAnnotation]
		if !ok {
			continue
		}
		key := types.NamespacedName{Namespace: pod.Namespace, Name: wlName}
		if seen[key] {
			continue
		}
		seen[key] = true

		// Never cycle the hero its own drain serves: once admitted, its
		// pods start on the still-tainted nodes and would otherwise be
		// mistaken for victims.
		if key.Namespace == heroWL.Namespace && key.Name == heroWL.Name {
			continue
		}

		victim := &kueue.Workload{}
		if err := r.Get(ctx, key, victim); err != nil {
			continue // gone = vacated
		}
		// Never suspend any hero-classed workload. Selection excluded domains
		// hosting other heroes, but that was checked at planning time — another
		// hero can be admitted into our drained domain afterwards (heroes
		// tolerate the taint). This check is the only protection in that window.
		if ref := victim.Spec.PriorityClassRef; ref != nil && ref.Name == r.Cfg.HeroPriorityClassName {
			continue
		}
		if victim.Spec.Active != nil && !*victim.Spec.Active {
			pending++ // deactivated, pods still terminating
			continue
		}
		if err := r.deactivateVictim(ctx, victim, heroWL, ownerRef); err != nil {
			return ctrl.Result{}, err
		}
		suspended++
		pending++
	}
	logf.FromContext(ctx).V(2).Info("victim cycle pass", "hero", ownerRef,
		"taintedNodes", len(taintedNodes), "suspended", suspended, "pendingEviction", pending)
	if suspended > 0 {
		r.Recorder.Eventf(heroWL, corev1.EventTypeNormal, EventVictimsSuspended,
			"suspended %d victim workloads on tainted nodes", suspended)
	}

	// Reactivate our evicted victims so Kueue requeues them elsewhere.
	all := &kueue.WorkloadList{}
	if err := r.List(ctx, all); err != nil {
		return ctrl.Result{}, err
	}
	for i := range all.Items {
		victim := &all.Items[i]
		if victim.Annotations[DeactivatedForAnnotation] != ownerRef {
			continue
		}
		if victim.Spec.Active != nil && !*victim.Spec.Active && evictedByDeactivation(victim) {
			if err := r.reactivateVictim(ctx, victim); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	if pending > 0 {
		return ctrl.Result{RequeueAfter: requeueVictimCycling}, nil
	}

	// Every victim is gone from the tainted nodes: the drained capacity
	// is real now.
	return r.nudgeWhilePending(ctx, heroWL, taintedNodes)
}

// nudgeWhilePending wakes kueue after the drain has freed its domain.
// Kueue does not notice the freed capacity on its own (why: see
// taint.NudgeAnnotation), so we bump an annotation on one drained node,
// immediately at first and then every NudgeInterval, until the hero is
// admitted or the janitor times the drain out.
func (r *Reconciler) nudgeWhilePending(ctx context.Context, heroWL *kueue.Workload, taintedNodes []string) (ctrl.Result, error) {
	if heroWL.Status.Admission != nil || len(taintedNodes) == 0 {
		return ctrl.Result{}, nil
	}
	nudgeNode := slices.Min(taintedNodes) // deterministic across passes
	if bumped, err := taint.NudgeKueue(ctx, r.Client, nudgeNode, r.Now(), r.Cfg.NudgeInterval.Duration); err != nil {
		return ctrl.Result{}, err
	} else if bumped {
		logf.FromContext(ctx).V(1).Info("nudged kueue to retry the hero",
			"hero", types.NamespacedName{Namespace: heroWL.Namespace, Name: heroWL.Name},
			"node", nudgeNode)
	}
	return ctrl.Result{RequeueAfter: r.Cfg.NudgeInterval.Duration}, nil
}

// reconcileVictimEvent runs when a watched Workload carries our
// deactivated-for annotation: finish its suspend/unsuspend cycle without
// waiting for the hero's own requeue tick.
func (r *Reconciler) reconcileVictimEvent(ctx context.Context, victim *kueue.Workload, ownerRef string) (ctrl.Result, error) {
	if victim.Spec.Active == nil || *victim.Spec.Active {
		return ctrl.Result{}, nil // already active; marker cleanup rides the next patch
	}

	// Orphaned marker: the owning hero no longer exists (deleted in the
	// narrow window between this victim being suspended and the janitor's
	// teardown sweep). No taints remain to re-trigger the janitor, so
	// reactivate unconditionally here — this handler fires on the
	// victim's own events, including the suspension patch itself.
	ns, name, ok := strings.Cut(ownerRef, "/")
	if ok {
		heroWL := &kueue.Workload{}
		err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, heroWL)
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.reactivateVictim(ctx, victim)
		}
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	if evictedByDeactivation(victim) {
		return ctrl.Result{}, r.reactivateVictim(ctx, victim)
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) deactivateVictim(ctx context.Context, victim, heroWL *kueue.Workload, ownerRef string) error {
	patch := client.MergeFrom(victim.DeepCopy())
	active := false
	victim.Spec.Active = &active
	if victim.Annotations == nil {
		victim.Annotations = map[string]string{}
	}
	victim.Annotations[DeactivatedForAnnotation] = ownerRef
	if err := r.Patch(ctx, victim, patch); err != nil {
		return err
	}
	metrics.VictimsSuspended.Inc()
	// UID, not name: the hero may live in another namespace (see the
	// DrainQueued event for the convention).
	r.Recorder.Eventf(victim, corev1.EventTypeNormal, EventSuspendedForHero,
		"suspended to make room for a hero workload (UID: %s); will be reactivated and requeued elsewhere once evicted", heroWL.UID)
	return nil
}

func (r *Reconciler) reactivateVictim(ctx context.Context, victim *kueue.Workload) error {
	return victims.Reactivate(ctx, r.Client, r.Recorder, victim)
}

// evictedByDeactivation reports Kueue finished evicting the workload after
// we set spec.active=false (Evicted=True with the Deactivated reason
// family; kueue may suffix a cause).
func evictedByDeactivation(wl *kueue.Workload) bool {
	c := meta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadEvicted)
	return c != nil && c.Status == metav1.ConditionTrue &&
		(c.Reason == kueue.WorkloadDeactivated || len(c.Reason) > len(kueue.WorkloadDeactivated) &&
			c.Reason[:len(kueue.WorkloadDeactivated)] == kueue.WorkloadDeactivated)
}

// abortDrain unwinds an in-flight drain whose hero can no longer fit even
// after full eviction: unpause every victim this drain suspended —
// unconditionally, without waiting for kueue's eviction to complete (a
// never-executed suspension is cancelled; an executed one requeues) — and
// remove this hero's taints from every node. The hero stays stuck; the
// next evaluation starts from a clean slate.
func (r *Reconciler) abortDrain(ctx context.Context, heroWL *kueue.Workload, own *taint.Drain, reason string) (ctrl.Result, error) {
	if _, err := victims.SweepFor(ctx, r.Client, r.Recorder, types.NamespacedName{
		Namespace: heroWL.Namespace, Name: heroWL.Name,
	}); err != nil {
		return ctrl.Result{}, err
	}

	for _, nodeName := range own.Nodes {
		if _, err := taint.RemoveOwnedTaint(ctx, r.Client, nodeName, r.Cfg.TaintKey, types.NamespacedName{
			Namespace: heroWL.Namespace, Name: heroWL.Name,
		}); err != nil {
			return ctrl.Result{}, err
		}
	}
	metrics.DrainsCompleted.WithLabelValues(metrics.OutcomeAborted).Inc()
	logf.FromContext(ctx).Info("drain aborted", "hero",
		types.NamespacedName{Namespace: heroWL.Namespace, Name: heroWL.Name},
		"nodes", len(own.Nodes), "reason", reason)
	r.Recorder.Eventf(heroWL, corev1.EventTypeWarning, EventDrainAborted,
		"drain aborted: %s; victims reactivated and taints removed, will re-evaluate", reason)
	return ctrl.Result{RequeueAfter: requeueWhileQueued}, nil
}

// handleNonHero finishes the reconcile of a workload that fails the hero
// checks. Normally that is a quiet exit — but a drain may be in flight for
// a workload that stopped being a hero mid-drain (CQ label or priority
// class changed). The hero markers are the AUTHORIZATION to evict other
// tenants; revoked mid-drain means stop evicting for it — abort rather
// than freeze until the timeout, even though kueue might still admit it.
func (r *Reconciler) handleNonHero(ctx context.Context, wl *kueue.Workload, reason hero.NotHeroReason) (ctrl.Result, error) {
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes); err != nil {
		return ctrl.Result{}, err
	}
	key := types.NamespacedName{Namespace: wl.Namespace, Name: wl.Name}
	if own := taint.FindDrains(nodes.Items, r.Cfg.TaintKey)[key]; own != nil {
		return r.abortDrain(ctx, wl, own, "workload is no longer a hero: "+string(reason))
	}
	logf.FromContext(ctx).V(3).Info("not a hero", "reason", reason)
	return ctrl.Result{}, nil
}

// refuseOverQuota is the Phase 0 quota precondition — the customer
// commitment that a hero always fits its CQ's nominal quota. Draining can
// only fix physical placement, never quota, so an over-quota hero gets a
// warning event instead of a drain (and violation risks the hero itself
// being preempted after admission). Nil result = within quota, continue
// reconciling; non-nil = stop with that result. If a drain is already in
// flight (quota shrank mid-drain), it is aborted: over nominal quota kueue
// will never admit the hero however empty the domain gets.
func (r *Reconciler) refuseOverQuota(ctx context.Context, wl *kueue.Workload, cq *kueue.ClusterQueue, own *taint.Drain) (*ctrl.Result, error) {
	heroGPU := hero.GPURequest(wl, r.Cfg.GPUResourceName)
	nominal := gpuNominalQuota(cq, r.Cfg.GPUResourceName)
	if heroGPU.Cmp(nominal) <= 0 {
		return nil, nil
	}
	r.Recorder.Eventf(wl, corev1.EventTypeWarning, EventHeroExceedsQuota,
		"hero requests %s %s but ClusterQueue %s nominal quota is %s; draining cannot help",
		heroGPU.String(), r.Cfg.GPUResourceName, cq.Name, nominal.String())
	if own != nil {
		res, err := r.abortDrain(ctx, wl, own, "hero exceeds nominal quota")
		return &res, err
	}
	return &ctrl.Result{}, nil
}

// drainNodeCount is the node count of a possibly absent drain, for logging.
func drainNodeCount(d *taint.Drain) int {
	if d == nil {
		return 0
	}
	return len(d.Nodes)
}

// workloadKey is the key of a possibly absent workload, for logging.
func workloadKey(wl *kueue.Workload) types.NamespacedName {
	if wl == nil {
		return types.NamespacedName{}
	}
	return types.NamespacedName{Namespace: wl.Namespace, Name: wl.Name}
}

// formatDemand renders the hero's demand chunks as "<count>x<size>" for
// logging; resource.Quantity does not print readably on its own.
func formatDemand(chunks []hero.Chunk) []string {
	out := make([]string, 0, len(chunks))
	for _, c := range chunks {
		out = append(out, fmt.Sprintf("%dx%s", c.Count, c.Size.String()))
	}
	return out
}

// selectionOutcomeLabel maps a SelectDomains result to its metric label.
func selectionOutcomeLabel(plan *selection.DrainPlan, reason selection.InfeasibleReason) string {
	switch {
	case plan != nil:
		return metrics.SelectionPlanned
	case reason == selection.AllFeasibleHeroOccupied:
		return metrics.SelectionAllFeasibleOccupied
	default:
		return metrics.SelectionNoFeasibleDomains
	}
}

// reportDrainQueued records the queued event on a waiting hero. Events must
// not leak other namespaces' workload names; the blocking hero is referenced
// by UID only (kueue's own convention, e.g. its Preempted event). Logs may
// carry the name.
func (r *Reconciler) reportDrainQueued(ctx context.Context, wl *kueue.Workload, blocking *taint.Drain) {
	blockingUID := "UNKNOWN"
	blockingWL := &kueue.Workload{}
	if err := r.Get(ctx, blocking.Owner, blockingWL); err == nil {
		blockingUID = string(blockingWL.UID)
	}
	logf.FromContext(ctx).V(1).Info("drain in flight; queued", "blockingHero", blocking.Owner)
	r.Recorder.Eventf(wl, corev1.EventTypeNormal, EventDrainQueued,
		"drain in flight for another hero workload (UID: %s, %d nodes); queued", blockingUID, len(blocking.Nodes))
}

// infeasibleResponse maps a selection failure to the event to record on
// the hero and the reconcile result to return.
func infeasibleResponse(reason selection.InfeasibleReason, level string) (eventType, eventReason, message string, result ctrl.Result) {
	switch reason {
	case selection.AllFeasibleHeroOccupied:
		// Woken promptly when the blocking hero changes state (see
		// mapHeroEventToStuck); the requeue is the safety net.
		return corev1.EventTypeNormal, EventAllFeasibleHeroOccupied,
			fmt.Sprintf("every feasible domain at level %s hosts another hero; waiting", level),
			ctrl.Result{RequeueAfter: requeueWhileQueued}
	default: // selection.NoFeasibleDomains
		return corev1.EventTypeWarning, EventNoFeasibleDomains,
			fmt.Sprintf("no feasible domain set at level %s covers the hero's demand; draining cannot help", level),
			ctrl.Result{RequeueAfter: requeueNoFeasible}
	}
}

// clusterQueueFor resolves the ClusterQueue a workload targets: the
// admitted CQ when present, else spec.queueName -> LocalQueue ->
// ClusterQueue. Nil (not an error) when the chain is broken — such a
// workload cannot be admitted and is not a hero.
func (r *Reconciler) clusterQueueFor(ctx context.Context, wl *kueue.Workload) (*kueue.ClusterQueue, error) {
	var cqName string
	if wl.Status.Admission != nil {
		cqName = string(wl.Status.Admission.ClusterQueue)
	} else if wl.Spec.QueueName != "" {
		lq := &kueue.LocalQueue{}
		err := r.Get(ctx, types.NamespacedName{Namespace: wl.Namespace, Name: string(wl.Spec.QueueName)}, lq)
		if err != nil {
			return nil, client.IgnoreNotFound(err)
		}
		cqName = string(lq.Spec.ClusterQueue)
	}
	if cqName == "" {
		return nil, nil
	}
	cq := &kueue.ClusterQueue{}
	if err := r.Get(ctx, types.NamespacedName{Name: cqName}, cq); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	return cq, nil
}

// drainLevel resolves two levels: the topology level to drain at — the
// coarsest of the hero's slice-required levels, validated against the
// Topology object reached via the CQ's ResourceFlavors — and the level
// selection must keep every drained domain inside (empty = no constraint).
//
// The grouping level comes from the hero's own podset `required` topology,
// NOT from the position of the drain level in the hierarchy. Kueue forces
// slices to share an ancestor only when the podset asks for one; absent
// that, it spreads them wherever they fit. Grouping by the hierarchy's
// next level up regardless would reject drains kueue would admit — a hero
// needing more slice domains than any single parent holds is then reported
// NoFeasibleDomains even with ample capacity spread across parents.
func (r *Reconciler) drainLevel(ctx context.Context, wl *kueue.Workload, cq *kueue.ClusterQueue) (string, string, *ctrl.Result) {
	required := hero.RequiredTopologyLevels(wl)
	if len(required) == 0 {
		r.Recorder.Event(wl, corev1.EventTypeWarning, EventTopologyLevelUnknown,
			"hero has no slice topology requirement (podset-slice-required-topology + podset-slice-size); nothing to drain for")
		return "", "", &ctrl.Result{}
	}

	topoLevels := r.topologyLevels(ctx, cq)
	if len(topoLevels) == 0 {
		r.Recorder.Event(wl, corev1.EventTypeWarning, EventTopologyLevelUnknown,
			"no Topology object reachable via the ClusterQueue's ResourceFlavors")
		return "", "", &ctrl.Result{}
	}
	level, ok := hero.CoarsestLevel(required, topoLevels)
	if !ok {
		r.Recorder.Eventf(wl, corev1.EventTypeWarning, EventTopologyLevelUnknown,
			"required level(s) %v not in Topology hierarchy %v", required, topoLevels)
		return "", "", &ctrl.Result{}
	}
	groupingLevel, ok := hero.GroupingLevel(wl, topoLevels, level)
	if !ok {
		r.Recorder.Eventf(wl, corev1.EventTypeWarning, EventTopologyLevelUnknown,
			"podset required level not in Topology hierarchy %v", topoLevels)
		return "", "", &ctrl.Result{}
	}
	return level, groupingLevel, nil
}

// topologyLevels walks CQ -> ResourceFlavor -> Topology and returns the
// first topology's ordered level keys.
func (r *Reconciler) topologyLevels(ctx context.Context, cq *kueue.ClusterQueue) []string {
	for _, rg := range cq.Spec.ResourceGroups {
		for _, fq := range rg.Flavors {
			rf := &kueue.ResourceFlavor{}
			if err := r.Get(ctx, types.NamespacedName{Name: string(fq.Name)}, rf); err != nil {
				continue
			}
			if rf.Spec.TopologyName == nil {
				continue
			}
			topo := &kueue.Topology{}
			if err := r.Get(ctx, types.NamespacedName{Name: string(*rf.Spec.TopologyName)}, topo); err != nil {
				continue
			}
			levels := make([]string, 0, len(topo.Spec.Levels))
			for _, l := range topo.Spec.Levels {
				levels = append(levels, l.NodeLabel)
			}
			return levels
		}
	}
	return nil
}

// stuckHeroes filters the workload list to stuck heroes (cluster-wide,
// across every hero-enabled CQ — serialization is per cluster).
func (r *Reconciler) stuckHeroes(ctx context.Context, all []kueue.Workload) ([]kueue.Workload, error) {
	var stuck []kueue.Workload
	for i := range all {
		wl := &all[i]
		// Deactivated workloads keep their stuck condition but can never
		// be drained for (see the Reconcile gate); including one here
		// would park it at the front of the NextHero line and block every
		// other hero forever.
		if wl.Spec.Active != nil && !*wl.Spec.Active {
			continue
		}
		if !hero.IsStuckTASNoFit(wl, r.Cfg.StuckDetection) {
			continue
		}
		cq, err := r.clusterQueueFor(ctx, wl)
		if err != nil {
			return nil, err
		}
		if ok, _ := hero.IsHero(wl, cq, r.Cfg); ok {
			stuck = append(stuck, *wl)
		}
	}
	return stuck, nil
}

// otherAdmittedHeroes returns admitted heroes other than self — their
// domains are off-limits to this drain.
func (r *Reconciler) otherAdmittedHeroes(ctx context.Context, all []kueue.Workload, self types.NamespacedName) ([]kueue.Workload, error) {
	var out []kueue.Workload
	for i := range all {
		wl := &all[i]
		if (wl.Namespace == self.Namespace && wl.Name == self.Name) || wl.Status.Admission == nil {
			continue
		}
		// A finished hero keeps its admission in status but its pods are
		// gone — it no longer occupies the domain.
		if meta.IsStatusConditionTrue(wl.Status.Conditions, kueue.WorkloadFinished) {
			continue
		}
		cq, err := r.clusterQueueFor(ctx, wl)
		if err != nil {
			return nil, err
		}
		if ok, _ := hero.IsHero(wl, cq, r.Cfg); ok {
			out = append(out, *wl)
		}
	}
	return out, nil
}

// gpuNominalQuota sums the CQ's nominal quota for the GPU resource across
// all resource groups and flavors.
func gpuNominalQuota(cq *kueue.ClusterQueue, gpu corev1.ResourceName) resource.Quantity {
	sum := resource.Quantity{}
	for _, rg := range cq.Spec.ResourceGroups {
		for _, fq := range rg.Flavors {
			for _, rq := range fq.Resources {
				if rq.Name == gpu {
					sum.Add(rq.NominalQuota)
				}
			}
		}
	}
	return sum
}

// SetupWithManager wires the controller: hero Workloads are the primary;
// victim Workloads map back to their owning drain via annotation; Node
// changes map to the nodes' drain owner. MaxConcurrentReconciles is pinned
// to 1 — the one-drain-at-a-time gate must never race itself.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	builder := ctrl.NewControllerManagedBy(mgr).
		Named("hero-drain").
		For(&kueue.Workload{}).
		Watches(&kueue.Workload{}, handler.EnqueueRequestsFromMapFunc(r.mapHeroEventToStuck)).
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(r.mapNodeToOwner)).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1})
	if r.Nudge != nil {
		builder = builder.WatchesRawSource(source.Channel(r.Nudge,
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, _ client.Object) []ctrl.Request {
				return r.stuckRequests(ctx)
			})))
	}
	return builder.Complete(r)
}

// mapNodeToOwner routes node events: a node carrying our taint re-triggers
// the drain that owns it; any other node change (capacity, health, labels)
// re-triggers every stuck workload — cluster capacity just moved, so stuck
// heroes must re-evaluate. Cheap condition-only filter here; the reconcile
// itself re-verifies hero-ness properly.
func (r *Reconciler) mapNodeToOwner(ctx context.Context, obj client.Object) []ctrl.Request {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil
	}
	if owner, ok := taint.Owner(node, r.Cfg.TaintKey); ok {
		return []ctrl.Request{{NamespacedName: owner}}
	}

	return r.stuckRequests(ctx)
}

// stuckRequests enqueues every workload that looks stuck on a TAS no-fit
// (cheap condition check; the reconcile itself re-verifies hero-ness).
func (r *Reconciler) stuckRequests(ctx context.Context) []ctrl.Request {
	workloads := &kueue.WorkloadList{}
	if err := r.List(ctx, workloads); err != nil {
		return nil
	}
	var reqs []ctrl.Request
	for i := range workloads.Items {
		wl := &workloads.Items[i]
		if hero.IsStuckTASNoFit(wl, r.Cfg.StuckDetection) {
			reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{
				Namespace: wl.Namespace, Name: wl.Name,
			}})
		}
	}
	return reqs
}

// mapHeroEventToStuck wakes every stuck workload when a hero-classed
// workload changes state (admitted, finished, deleted): domains it occupied
// may have opened up. Cheap spec check only — hero events are rare.
func (r *Reconciler) mapHeroEventToStuck(ctx context.Context, obj client.Object) []ctrl.Request {
	wl, ok := obj.(*kueue.Workload)
	if !ok {
		return nil
	}
	ref := wl.Spec.PriorityClassRef
	if ref == nil || ref.Name != r.Cfg.HeroPriorityClassName {
		return nil
	}
	return r.stuckRequests(ctx)
}
