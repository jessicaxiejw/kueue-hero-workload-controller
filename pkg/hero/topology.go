// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package hero

import (
	"slices"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// RequiredTopologyLevels returns the distinct topology levels (node label
// keys) the workload's podsets require for draining, in podset order.
// Per the customer contract, heroes carry the slice pair — a podset
// contributes its level only when BOTH podSetSliceRequiredTopology and
// podSetSliceSize are set (kueue.x-k8s.io/podset-slice-required-topology +
// kueue.x-k8s.io/podset-slice-size, validated together by kueue's
// webhook): each slice must fit one domain at that level, and the workload
// pends forever on no-fit.
//
// Plain podset-required-topology is deliberately NOT a drain trigger.
// Podsets without the slice pair are skipped: empty result = workload is
// not drainable-for. When podsets disagree on the level, drain at the
// coarsest (see CoarsestLevel).
func RequiredTopologyLevels(wl *kueue.Workload) []string {
	var levels []string
	for i := range wl.Spec.PodSets {
		if !RequiresSliceTopology(&wl.Spec.PodSets[i]) {
			continue
		}
		level := *wl.Spec.PodSets[i].TopologyRequest.PodSetSliceRequiredTopology
		if !slices.Contains(levels, level) {
			levels = append(levels, level)
		}
	}
	return levels
}

// RequiresSliceTopology reports whether a podset carries the slice pair
// (podSetSliceRequiredTopology + podSetSliceSize) that makes it
// drain-relevant. Only these podsets land in a drained topology domain,
// so only they must tolerate the drain taint.
func RequiresSliceTopology(ps *kueue.PodSet) bool {
	tr := ps.TopologyRequest
	if tr == nil {
		return false
	}
	return tr.PodSetSliceRequiredTopology != nil && *tr.PodSetSliceRequiredTopology != "" &&
		tr.PodSetSliceSize != nil && *tr.PodSetSliceSize > 0
}

// GroupingLevel returns the topology level selection must keep a
// multi-domain drain inside, given the hero's drain level and the Topology
// hierarchy (ordered highest to lowest). Empty = no constraint: pack
// across the whole drain level. The bool is false only for a
// misconfiguration — a required level absent from the hierarchy.
//
// The constraint comes from the hero's own podset `required` topology, not
// from the drain level's position in the hierarchy. Kueue forces a
// podset's slices to share an ancestor only when the podset asks for one;
// otherwise it spreads them wherever they fit, and so may the drain.
// Grouping by the next level up regardless would reject drains kueue would
// admit whenever a hero needs more slice domains than one ancestor holds.
//
// A `required` level FINER than the drain level constrains placement
// inside a single drained domain, which any plan at the drain level
// already satisfies; it groups nothing and is reported as no constraint.
func GroupingLevel(wl *kueue.Workload, topologyLevels []string, drainLevel string) (string, bool) {
	required := groupingLevels(wl)
	if len(required) == 0 {
		return "", true
	}
	level, ok := finestLevel(required, topologyLevels)
	if !ok {
		return "", false
	}
	if slices.Index(topologyLevels, level) > slices.Index(topologyLevels, drainLevel) {
		return "", true
	}
	return level, true
}

// groupingLevels returns the distinct podset-level `required` topology
// levels (kueue.x-k8s.io/podset-required-topology) declared by the podsets
// that drive drain demand — those carrying the slice pair. Only such
// podsets can constrain the plan: a podset contributing no demand chunks
// has no say in which domains get drained.
func groupingLevels(wl *kueue.Workload) []string {
	var levels []string
	for i := range wl.Spec.PodSets {
		tr := wl.Spec.PodSets[i].TopologyRequest
		if tr == nil || tr.Required == nil || *tr.Required == "" {
			continue
		}
		if tr.PodSetSliceRequiredTopology == nil || *tr.PodSetSliceRequiredTopology == "" ||
			tr.PodSetSliceSize == nil || *tr.PodSetSliceSize <= 0 {
			continue
		}
		if !slices.Contains(levels, *tr.Required) {
			levels = append(levels, *tr.Required)
		}
	}
	return levels
}

// CoarsestLevel picks the drain level: the highest of the required levels
// in the Topology hierarchy. topologyLevels is the Topology object's
// spec.levels, ordered highest to lowest per the CRD contract. Coarsest
// wins because an emptied block satisfies any rack requirement inside it,
// never the converse. Returns false if a required level is not in the
// hierarchy (misconfiguration — surface it, don't drain).
func CoarsestLevel(required, topologyLevels []string) (string, bool) {
	best := -1
	for _, r := range required {
		idx := slices.Index(topologyLevels, r)
		if idx < 0 {
			return "", false
		}
		if best == -1 || idx < best {
			best = idx
		}
	}
	if best < 0 {
		return "", false
	}
	return topologyLevels[best], true
}

// finestLevel picks the grouping level when slice podsets disagree: the
// lowest of the required levels in the Topology hierarchy (ordered highest
// to lowest, as in CoarsestLevel). Finest wins because a domain at the
// finest level sits inside every coarser one, so honouring it honours them
// all — the mirror of CoarsestLevel's argument for the drain level.
// Returns false if a level is not in the hierarchy.
func finestLevel(required, topologyLevels []string) (string, bool) {
	best := -1
	for _, r := range required {
		idx := slices.Index(topologyLevels, r)
		if idx < 0 {
			return "", false
		}
		if idx > best {
			best = idx
		}
	}
	if best < 0 {
		return "", false
	}
	return topologyLevels[best], true
}
