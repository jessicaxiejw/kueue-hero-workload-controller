// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

// Package janitorctrl is the janitor: design doc phase 4. It watches nodes
// carrying the drain taint and tears drains down when their hero no longer
// needs them — structurally, from node state alone, so leaks self-heal
// even across controller restarts.
//
// Phase A implements the abandoned trigger (hero deleted or deactivated)
// plus the unconditional victim sweep at every teardown. Placed /
// placed-elsewhere / finished / timeout triggers follow in later phases.
package janitorctrl

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/index"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/metrics"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/taint"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/victims"
)

// EventDrainTimedOut is recorded on the hero when its drain is torn down
// for exceeding the configured DrainTimeout.
const EventDrainTimedOut = "DrainTimedOut"

// Reconciler tears down drains whose hero no longer needs them. Primary
// object is the Node: cleanup is keyed off what exists (tainted nodes),
// never off remembering what happened, so a fresh process reaches the same
// conclusions.
type Reconciler struct {
	client.Client
	Recorder record.EventRecorder
	Cfg      *config.Config
	Now      func() time.Time
	// Nudge, when set, receives an event after every teardown so the
	// drain controller re-evaluates queued heroes immediately instead of
	// on its polling requeue. Best-effort: a full channel is skipped (the
	// 30s requeue is the fallback).
	Nudge chan<- event.GenericEvent
}

// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile inspects ONE tainted node and answers a single question: does
// this node's drain taint still serve its hero? The checks run as a
// decision ladder — the first answer wins:
//
//  1. IDENTIFY  whose taint is this?            (unknown → not ours, stop)
//  2. ABANDONED does the hero still exist and want to run?  (no → teardown)
//     2c. or has it FINISHED (success or failure)?          (yes → teardown)
//  3. PLACED    did the hero land somewhere?
//     3a. elsewhere → this node's taint serves nothing   (teardown, now)
//     3b. here      → wait until EVERY hero pod is Running, then teardown
//  4. TIMEOUT   the drain deadline bounds the WHOLE journey — taint →
//     eviction → admission → all pods Running. Reached both when the hero
//     is not admitted and when it is admitted here but its pods never all
//     start (bad image, unschedulable pods, a constraint kueue's
//     admission did not model). Teardown and warn the hero (no retry
//     backoff, by design: repeated timeouts surface via events and
//     metrics instead).
//
// Teardown always means: remove this node's drain taint + annotations, and
// sweep the drain's suspended victims back to active.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("node", req.Name)

	node := &corev1.Node{}
	if err := r.Get(ctx, req.NamespacedName, node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err) // node gone = taint gone
	}

	// ── 1. IDENTIFY ────────────────────────────────────────────────────
	// Owner comes from the drain-owner annotation (the taint's VALUE is
	// the ClusterQueue, for scheduling only). The lookup searches for our
	// exact taint shape — foreign taints on the same node never shadow
	// ours. Our key without a resolvable annotation = not written by this
	// controller: never blocked on, never removed.
	owner, ok := taint.Owner(node, r.Cfg.TaintKey)
	if !ok {
		return ctrl.Result{}, nil
	}

	// ── 2. ABANDONED ───────────────────────────────────────────────────
	// 2a. Hero deleted: the drain serves nothing.
	hero := &kueue.Workload{}
	err := r.Get(ctx, owner, hero)
	switch {
	case apierrors.IsNotFound(err):
		log.Info("drain owner no longer exists; tearing down", "hero", owner)
		return r.teardown(ctx, node.Name, owner, metrics.OutcomeAbandoned)
	case err != nil:
		return ctrl.Result{}, err
	}
	// 2b. Hero deactivated (spec.active=false, by anyone): same.
	if hero.Spec.Active != nil && !*hero.Spec.Active {
		log.Info("drain owner is deactivated; tearing down", "hero", owner)
		return r.teardown(ctx, node.Name, owner, metrics.OutcomeDeactivated)
	}
	// 2c. Hero finished (success or failure): normally the placed trigger
	// fired long ago; this matters when the hero ran or failed so fast it
	// never did. Checked before PLACED so a finished hero's stale
	// admission cannot route us into pod counting.
	if meta.IsStatusConditionTrue(hero.Status.Conditions, kueue.WorkloadFinished) {
		log.Info("drain owner finished; tearing down", "hero", owner)
		return r.teardown(ctx, node.Name, owner, metrics.OutcomeFinished)
	}

	// ── 3. PLACED ──────────────────────────────────────────────────────
	if hero.Status.Admission != nil {
		// 3a. The admission's topology assignment does not include this
		// node: the hero landed elsewhere, this node's taint holds
		// capacity the hero will never use. Untaint at admission.
		if !assignmentTouchesNode(hero.Status.Admission, node) {
			log.Info("hero placed elsewhere; tearing down this node", "hero", owner)
			return r.teardown(ctx, node.Name, owner, metrics.OutcomePlacedElsewhere)
		}
		// 3b. Assignment includes this node: hold the taint until
		// running pods >= admitted pod count. NOT at mere admission —
		// between admission and Running, freed capacity could let stray
		// non-kueue pods slip into the domain.
		running, err := r.runningHeroPods(ctx, hero)
		if err != nil {
			return ctrl.Result{}, err
		}
		if running >= admittedPodCount(hero) {
			log.Info("hero fully running; tearing down", "hero", owner, "running", running)
			return r.teardown(ctx, node.Name, owner, metrics.OutcomePlaced)
		}
		// Admitted here but not fully running: fall through to the timeout.
		// Admission is not the finish line — the taints protect the freed
		// capacity until the pods actually run, and that stretch must not
		// be unbounded either.
	}

	// ── 4. TIMEOUT ─────────────────────────────────────────────────────
	// The drain is still working toward a fully running hero — but not
	// forever. The clock is the drain-started-at annotation written with
	// the taint.
	startedAt, ok := drainStartedAt(node)
	if !ok {
		// Clock lost (annotation stripped): restart it rather than guess
		// — never an instant timeout.
		log.Info("drain-started-at missing; restarting the clock", "hero", owner)
		if err := r.restartClock(ctx, node.Name); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: r.Cfg.DrainTimeout.Duration}, nil
	}
	remaining := r.Cfg.DrainTimeout.Duration - r.Now().Sub(startedAt)
	if remaining > 0 {
		// Deadline not reached: arm a wake-up for the exact moment it is
		// — the janitor's only time-based trigger, needs no event.
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	// Deadline passed: the drain is not converging (victims will not
	// move, capacity math was wrong, ...). Tear down and warn the hero.
	// Deliberately NO retry backoff (deviation from the design doc, for
	// scheduling simplicity): the hero is immediately eligible for a
	// fresh drain. Repeated timeouts are an operator signal — the
	// DrainTimedOut events and the drains-timed-out metric (M9) are the
	// investigation trail.
	log.Info("drain timed out; tearing down", "hero", owner, "startedAt", startedAt)
	r.Recorder.Eventf(hero, corev1.EventTypeWarning, EventDrainTimedOut,
		"drain did not converge within %s; taints removed, drain will be re-evaluated",
		r.Cfg.DrainTimeout.Duration)
	return r.teardown(ctx, node.Name, owner, metrics.OutcomeTimedOut)
}

// drainStartedAt reads the per-node drain clock.
func drainStartedAt(node *corev1.Node) (time.Time, bool) {
	raw, ok := node.Annotations[taint.StartedAtAnnotation]
	if !ok {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// restartClock rewrites a lost drain-started-at annotation as now.
func (r *Reconciler) restartClock(ctx context.Context, nodeName string) error {
	node := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		return client.IgnoreNotFound(err)
	}
	patch := client.MergeFrom(node.DeepCopy())
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	node.Annotations[taint.StartedAtAnnotation] = r.Now().UTC().Format(time.RFC3339)
	return r.Patch(ctx, node, patch)
}

// assignmentTouchesNode reports whether the hero's topology assignment
// places any pods on this node. Hostname-lowest assignments match by node
// name (kueue's own helper); coarser assignments match the node's label at
// the assignment's lowest level.
func assignmentTouchesNode(adm *kueue.Admission, node *corev1.Node) bool {
	if utiltas.HasTASAssignmentOnNode(adm, node.Name) {
		return true
	}
	for i := range adm.PodSetAssignments {
		ta := adm.PodSetAssignments[i].TopologyAssignment
		if ta == nil || len(ta.Levels) == 0 || utiltas.IsLowestLevelHostname(ta.Levels) {
			continue // nil or already handled by the hostname helper
		}
		nodeValue, ok := node.Labels[ta.Levels[len(ta.Levels)-1]]
		if !ok {
			continue
		}
		for domain := range utiltas.InternalSeqFrom(ta) {
			if len(domain.Values) > 0 && domain.Values[len(domain.Values)-1] == nodeValue && domain.Count > 0 {
				return true
			}
		}
	}
	return false
}

// runningHeroPods counts the hero's Running pods (kueue stamps the
// kueue.x-k8s.io/workload annotation on them at job start).
func (r *Reconciler) runningHeroPods(ctx context.Context, hero *kueue.Workload) (int32, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(hero.Namespace),
		client.MatchingFields{index.PodWorkload: hero.Name}); err != nil {
		return 0, err
	}
	var running int32
	for i := range pods.Items {
		if pods.Items[i].Status.Phase == corev1.PodRunning {
			running++
		}
	}
	return running, nil
}

// admittedPodCount is the pod count the drain must see Running: the
// admitted counts when present (partial admission can reduce below spec),
// spec counts otherwise.
func admittedPodCount(hero *kueue.Workload) int32 {
	var total int32
	if hero.Status.Admission != nil {
		for i := range hero.Status.Admission.PodSetAssignments {
			if c := hero.Status.Admission.PodSetAssignments[i].Count; c != nil {
				total += *c
			}
		}
	}
	if total > 0 {
		return total
	}
	for i := range hero.Spec.PodSets {
		total += hero.Spec.PodSets[i].Count
	}
	return total
}

// teardown removes this node's drain taint and sweeps the drain's victims
// back to active. It runs once per node and is idempotent.
//
// Completion metrics count drains, not nodes: only the teardown of the
// drain's LAST tainted node increments them, labeled with that node's
// teardown reason. Counting happens BEFORE the removal on purpose: the
// "am I the last?" check reads the informer cache, which lags our own
// writes, so checking after the removal can miss the count entirely (the
// cache still shows our taint, we assume a later teardown will count, and
// on the true last node there is none). The trade is at-least-once: a
// crash between count and removal recounts on retry. For an alert counter
// a rare extra increment beats a silently missing one.
func (r *Reconciler) teardown(ctx context.Context, nodeName string, owner types.NamespacedName, outcome string) (ctrl.Result, error) {
	tainted := &corev1.NodeList{}
	if err := r.List(ctx, tainted, client.MatchingFields{index.NodeDrainTainted: index.True}); err != nil {
		return ctrl.Result{}, err
	}
	drains := taint.FindDrains(tainted.Items, r.Cfg.TaintKey)
	own := drains[owner]
	last := own != nil && len(own.Nodes) == 1 && own.Nodes[0] == nodeName
	if last {
		metrics.DrainsCompleted.WithLabelValues(outcome).Inc()
		if outcome == metrics.OutcomeTimedOut {
			metrics.DrainsTimedOut.Inc()
		}
	}
	inFlight := len(drains)
	if last {
		inFlight-- // this drain ends with the removal below
	}
	metrics.DrainsInFlight.Set(float64(inFlight))

	if _, err := taint.RemoveOwnedTaint(ctx, r.Client, nodeName, r.Cfg.TaintKey, owner); err != nil {
		return ctrl.Result{}, err
	}
	if _, err := victims.SweepFor(ctx, r.Client, r.Recorder, owner); err != nil {
		return ctrl.Result{}, err
	}
	if r.Nudge != nil {
		select {
		case r.Nudge <- event.GenericEvent{}:
		default: // full channel: queued heroes fall back to their requeue
		}
	}
	return ctrl.Result{}, nil
}

// SetupWithManager wires the janitor: nodes carrying the drain taint key
// are the primary; hero Workload deletions map to their tainted nodes so
// teardown is prompt rather than resync-bound.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	hasDrainTaint := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		node, ok := obj.(*corev1.Node)
		if !ok {
			return false
		}
		for i := range node.Spec.Taints {
			if node.Spec.Taints[i].Key == r.Cfg.TaintKey &&
				node.Spec.Taints[i].Effect == corev1.TaintEffectNoSchedule {
				return true
			}
		}
		return false
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named("hero-janitor").
		For(&corev1.Node{}, builder.WithPredicates(hasDrainTaint)).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.mapPodToTaintedNodes)).
		Watches(&kueue.Workload{}, handler.Funcs{
			DeleteFunc: func(ctx context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
				for _, req := range r.taintedNodesOf(ctx, e.Object) {
					q.Add(req)
				}
			},
			UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
				for _, req := range r.taintedNodesOf(ctx, e.ObjectNew) {
					q.Add(req)
				}
			},
		}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

// mapPodToTaintedNodes routes pod events (hero pods reaching Running) to
// the nodes tainted for the pod's owning workload.
func (r *Reconciler) mapPodToTaintedNodes(ctx context.Context, obj client.Object) []ctrl.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	wlName, ok := pod.Annotations[kueue.WorkloadAnnotation]
	if !ok {
		return nil
	}
	return r.taintedNodesOfKey(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: wlName})
}

// taintedNodesOf maps a hero Workload event to the nodes its drain taints.
func (r *Reconciler) taintedNodesOf(ctx context.Context, obj client.Object) []ctrl.Request {
	wl, ok := obj.(*kueue.Workload)
	if !ok {
		return nil
	}
	return r.taintedNodesOfKey(ctx, types.NamespacedName{Namespace: wl.GetNamespace(), Name: wl.GetName()})
}

// taintedNodesOfKey resolves a hero to the nodes its drain taints. It runs
// on every pod event in the cluster (see mapPodToTaintedNodes), so it asks
// the drain-owner index rather than listing — and copying — every node:
// with no drain in flight for that hero it returns nothing at all.
func (r *Reconciler) taintedNodesOfKey(ctx context.Context, self types.NamespacedName) []ctrl.Request {
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes,
		client.MatchingFields{index.NodeDrainOwner: victims.OwnerRef(self)}); err != nil {
		return nil
	}
	reqs := make([]ctrl.Request, 0, len(nodes.Items))
	for i := range nodes.Items {
		reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{Name: nodes.Items[i].Name}})
	}
	return reqs
}
