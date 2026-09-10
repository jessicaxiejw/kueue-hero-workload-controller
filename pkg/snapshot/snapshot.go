// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

// Package snapshot turns cluster state (nodes, pods, other admitted heroes)
// into per-domain capacity and victim data — the substrate for feasibility
// and domain selection. Pure functions, no cluster access.
package snapshot

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/util/tas"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/taint"
)

// Snapshot is a flat, point-in-time view of the cluster sliced at exactly
// one topology level — the drain level chosen for the hero
// (hero.CoarsestLevel). A block-level snapshot contains only blocks; racks
// or hosts never appear as domains. There is no nested hierarchy here:
// each domain carries its GPU capacity, what a drain there would evict,
// and whether another hero occupies it. Built fresh each reconcile from
// cached objects; never stored between reconciles.
type Snapshot struct {
	// Level is the node label key domains are keyed by.
	Level string
	// Domains maps the label value at Level to that domain's state.
	Domains map[string]*Domain
}

// Domain is one topology domain (e.g. one block or one rack).
type Domain struct {
	// ID is the node label value at the snapshot's level.
	ID string
	// Group is the node label value at Input.GroupLevel — the ancestor
	// the hero's podset `required` topology forces all its slices to
	// share. Selection only combines domains with the same Group, keeping
	// such a multi-domain drain inside one required-level domain. Empty
	// when the hero declares no `required` level (kueue is then free to
	// spread the slices, so the drain must be too) or the nodes lack the
	// label; all such domains land in one unconstrained group.
	Group string
	// Nodes are all node names labeled into this domain, usable or not.
	// A drain taints every one of them.
	Nodes []string
	// AllocatableGPU sums the GPU allocatable of usable nodes only:
	// Ready, schedulable, and free of NoSchedule taints (a node the hero
	// cannot land on contributes no capacity).
	AllocatableGPU resource.Quantity
	// NonReclaimableGPU sums GPU requests, on usable nodes, of pods that
	// suspending Kueue workloads cannot move: any GPU pod without the
	// kueue.x-k8s.io/workload annotation (DaemonSets, static pods,
	// operators, ...) unless it carries a NonBlockingPodLabels entry.
	NonReclaimableGPU resource.Quantity
	// Victims are the Kueue-managed GPU pods in the domain — what a drain
	// of this domain evicts. Includes pods on unusable nodes: eviction is
	// domain-wide, so they count toward disruption cost even though their
	// nodes contribute no capacity.
	Victims []Victim
	// HasOtherHero reports that another hero workload has a topology
	// assignment inside this domain; such domains are never drained.
	HasOtherHero bool
}

// Victim is one Kueue-managed GPU pod a drain would evict.
type Victim struct {
	Pod types.NamespacedName
	// Node the pod runs on.
	Node string
	// Workload is the Kueue Workload owning the pod (namespace = pod's,
	// name from the kueue.x-k8s.io/workload annotation). M4 joins this to
	// Workload objects for priority/size/age cost terms.
	Workload types.NamespacedName
	// GPURequest is the pod's total GPU request.
	GPURequest resource.Quantity
}

// Input carries everything Build needs. The caller (reconciler) does all
// listing; Build stays pure.
type Input struct {
	// Level is the topology level to key domains by (from
	// hero.RequiredTopologyLevels + CoarsestLevel).
	Level string
	// GroupLevel is the node label key selection must keep a multi-domain
	// drain inside, derived from the hero's podset `required` topology
	// (hero.GroupingLevels + FinestLevel); empty when the hero requires
	// no such ancestor.
	GroupLevel string
	// Nodes are the cluster's nodes (Build ignores nodes missing the
	// Level label).
	Nodes []corev1.Node
	// Pods are the pods on those nodes (terminal pods are ignored).
	Pods []corev1.Pod
	// OtherHeroes are admitted hero Workloads other than the one being
	// drained for; their assignments mark domains as hero-occupied.
	OtherHeroes []kueue.Workload
	// Self is the hero this snapshot serves. Nodes carrying Self's own
	// drain taint count as USABLE — the hero tolerates its own taint and
	// will land there; without this, a reconcile after tainting would see
	// its own drained domain as unusable and select another one (runaway
	// drain). Foreign taints still disqualify.
	Self types.NamespacedName
	// Cfg supplies GPUResourceName, TaintKey and NonBlockingPodLabels.
	Cfg *config.Config
}

// Build constructs the snapshot.
func Build(in Input) *Snapshot {
	s := &Snapshot{Level: in.Level, Domains: map[string]*Domain{}}

	// Node name -> domain ID, and per-node usability.
	nodeDomain := make(map[string]string, len(in.Nodes))
	nodeUsable := make(map[string]bool, len(in.Nodes))
	for i := range in.Nodes {
		node := &in.Nodes[i]
		domainID, ok := node.Labels[in.Level]
		if !ok || domainID == "" {
			continue // not part of this topology level
		}
		nodeDomain[node.Name] = domainID
		usable := nodeIsUsable(node, in.Cfg.TaintKey, in.Self)
		nodeUsable[node.Name] = usable

		d := s.domain(domainID)
		d.Nodes = append(d.Nodes, node.Name)
		if in.GroupLevel != "" && d.Group == "" {
			d.Group = node.Labels[in.GroupLevel]
		}
		if usable {
			if q, ok := node.Status.Allocatable[in.Cfg.GPUResourceName]; ok {
				d.AllocatableGPU.Add(q)
			}
		}
	}

	for i := range in.Pods {
		pod := &in.Pods[i]
		domainID, ok := nodeDomain[pod.Spec.NodeName]
		if !ok {
			continue // unscheduled, or node outside this level
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		// A terminating pod occupies no future capacity: its GPUs are in
		// the process of freeing, and evicting its workload again would be
		// pure churn. This matters between back-to-back drain cycles (no
		// retry backoff): the previous cycle's evictees may still be
		// terminating on the domain's nodes when the next selection runs,
		// and counting them would inflate the domain's disruption cost and
		// mark the wrong workloads as victims. The pathological case (a
		// pod stuck terminating forever on a dead kubelet, making the
		// domain look freer than it is) is bounded by the drain timeout,
		// same as any other capacity that fails to materialize.
		if pod.DeletionTimestamp != nil {
			continue
		}
		gpu := podGPURequest(pod, in.Cfg.GPUResourceName)
		if gpu.IsZero() {
			// CPU/memory-only co-tenants neither block nor get counted
			// as victims: the hero's placement constraint is GPUs.
			continue
		}

		d := s.domain(domainID)
		switch classify(pod, in.Cfg) {
		case podVictim:
			d.Victims = append(d.Victims, Victim{
				Pod:        types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
				Node:       pod.Spec.NodeName,
				Workload:   types.NamespacedName{Namespace: pod.Namespace, Name: pod.Annotations[kueue.WorkloadAnnotation]},
				GPURequest: gpu,
			})
		case podNonReclaimable:
			if nodeUsable[pod.Spec.NodeName] {
				d.NonReclaimableGPU.Add(gpu)
			}
		case podNonBlocking:
			// Counted neither against capacity nor as a victim.
		}
	}

	markHeroDomains(s, in.OtherHeroes, nodeDomain)
	return s
}

func (s *Snapshot) domain(id string) *Domain {
	d, ok := s.Domains[id]
	if !ok {
		d = &Domain{ID: id}
		s.Domains[id] = d
	}
	return d
}

// nodeIsUsable reports whether the hero could land on this node: Ready,
// schedulable, and free of taints the hero cannot tolerate. The hero's OWN
// drain taint (key + owner match) does not disqualify — the hero tolerates
// it and the drained nodes are its destination. Everything else with
// NoSchedule/NoExecute does, including another hero's drain taint.
func nodeIsUsable(node *corev1.Node, taintKey string, self types.NamespacedName) bool {
	if node.Spec.Unschedulable {
		return false
	}
	for i := range node.Spec.Taints {
		t := &node.Spec.Taints[i]
		if t.Effect != corev1.TaintEffectNoSchedule && t.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		if t.Key == taintKey && self != (types.NamespacedName{}) {
			if owner, ok := taint.Owner(node, taintKey); ok && owner == self {
				continue // our own drain taint; the hero tolerates it
			}
		}
		return false
	}
	ready := false
	for i := range node.Status.Conditions {
		c := &node.Status.Conditions[i]
		if c.Type == corev1.NodeReady {
			ready = c.Status == corev1.ConditionTrue // last entry wins
		}
	}
	return ready // no Ready condition = unknown health = unusable
}

// markHeroDomains sets HasOtherHero on every domain where any podset of any
// other hero has topology-assigned pods. Every podSetAssignments entry is
// decoded — TAS assigns each podset independently, so one workload's
// podsets can occupy different domains. Assignments whose levels do not
// include the snapshot level (e.g. hostname-only assignments) are resolved
// through the node -> domain map.
func markHeroDomains(s *Snapshot, heroes []kueue.Workload, nodeDomain map[string]string) {
	for h := range heroes {
		admission := heroes[h].Status.Admission
		if admission == nil {
			continue
		}
		for p := range admission.PodSetAssignments {
			ta := admission.PodSetAssignments[p].TopologyAssignment
			if ta == nil {
				continue
			}
			levelIdx := -1
			for l, level := range ta.Levels {
				if level == s.Level {
					levelIdx = l
					break
				}
			}
			for domain := range tas.InternalSeqFrom(ta) {
				var id string
				if levelIdx >= 0 {
					id = domain.Values[levelIdx]
				} else if len(ta.Levels) > 0 && ta.Levels[len(ta.Levels)-1] == corev1.LabelHostname {
					// Hostname-only assignment: map the host to its domain.
					id = nodeDomain[domain.Values[len(domain.Values)-1]]
				}
				if d, ok := s.Domains[id]; ok && id != "" {
					d.HasOtherHero = true
				}
			}
		}
	}
}

func podGPURequest(pod *corev1.Pod, gpu corev1.ResourceName) resource.Quantity {
	total := resource.Quantity{}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if q, ok := c.Resources.Requests[gpu]; ok {
			total.Add(q)
		} else if q, ok := c.Resources.Limits[gpu]; ok {
			total.Add(q)
		}
	}
	return total
}
