// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

// Package hero decides which Workloads the drain controller acts on: pure
// predicates over Kueue Workload/ClusterQueue objects, no cluster access.
package hero

import (
	corev1 "k8s.io/api/core/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
)

// NotHeroReason explains why a Workload failed hero identification; emitted
// in logs at high verbosity, never as events (non-heroes are the common
// case, not an error).
type NotHeroReason string

const (
	ReasonIsHero             NotHeroReason = ""
	ReasonCQNotHero          NotHeroReason = "ClusterQueueNotHeroEnabled"
	ReasonWrongPriorityClass NotHeroReason = "NotHeroPriorityClass"
	ReasonMissingToleration  NotHeroReason = "MissingHeroTaintToleration"
	ReasonNoPriorityClassRef NotHeroReason = "NoPriorityClassRef"
)

// IsHero reports whether the Workload matches all three hero identifiers:
//
//  1. submitted through a ClusterQueue labeled <HeroCQLabelKey>: "true"
//  2. carrying the hero WorkloadPriorityClass name
//  3. every podset carrying the slice pair tolerates the hero taint key
//
// cq must be the ClusterQueue the Workload targets (spec.queueName's CQ or
// status.admission.clusterQueue); passing it in keeps this predicate pure.
func IsHero(wl *kueue.Workload, cq *kueue.ClusterQueue, cfg *config.Config) (bool, NotHeroReason) {
	// 1. submitted through a ClusterQueue labeled <HeroCQLabelKey>: "true"
	if cq == nil || cq.Labels[cfg.HeroCQLabelKey] != "true" {
		return false, ReasonCQNotHero
	}

	// 2. carrying the hero WorkloadPriorityClass
	ref := wl.Spec.PriorityClassRef
	if ref == nil {
		return false, ReasonNoPriorityClassRef
	}

	if ref.Kind != kueue.WorkloadPriorityClassKind ||
		ref.Group != kueue.WorkloadPriorityClassGroup ||
		ref.Name != cfg.HeroPriorityClassName {
		return false, ReasonWrongPriorityClass
	}

	// 3. every drain-relevant podset tolerates the drain taint as it will
	// actually be applied: key + the hero's own ClusterQueue as the
	// value. Equal on the own CQ is the recommended form (it keeps the
	// hero out of other CQs' drained domains); a bare Exists also passes
	// here. Podsets without the slice pair are never placed in a drained
	// domain, so their tolerations are irrelevant.
	heroTaint := &corev1.Taint{Key: cfg.TaintKey, Value: cq.Name, Effect: corev1.TaintEffectNoSchedule}
	for i := range wl.Spec.PodSets {
		if !RequiresSliceTopology(&wl.Spec.PodSets[i]) {
			continue
		}
		if !toleratesTaint(wl.Spec.PodSets[i].Template.Spec.Tolerations, heroTaint) {
			return false, ReasonMissingToleration
		}
	}
	return true, ReasonIsHero
}

// toleratesTaint reports whether any toleration in the list tolerates the
// taint, implementing the core scheduling semantics (empty effect matches
// all effects; empty key with Exists matches all taints; Exists matches any
// value). Implemented locally rather than via corev1.Toleration's
// ToleratesTaint, whose signature keeps changing across k8s.io/api minors
// (a klog.Logger and an alpha comparison-operators flag were added in 0.35).
func toleratesTaint(tolerations []corev1.Toleration, taint *corev1.Taint) bool {
	for i := range tolerations {
		t := &tolerations[i]
		if len(t.Effect) > 0 && t.Effect != taint.Effect {
			continue
		}
		if len(t.Key) > 0 && t.Key != taint.Key {
			continue
		}
		switch t.Operator {
		case corev1.TolerationOpExists:
			return true
		case "", corev1.TolerationOpEqual:
			if t.Value == taint.Value {
				return true
			}
		}
	}
	return false
}
