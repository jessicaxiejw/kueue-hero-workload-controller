// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package hero

import (
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
)

const (
	// reasonTopologyPlacementFailed is the granular QuotaReserved=False
	// condition reason kueue >= 0.19 stamps on a TAS no-fit
	// (WorkloadQuotaReservedReasonTopologyPlacementFailed upstream). The
	// constant does not exist in the pinned kueue 0.16.9 module, so it is
	// declared here; kueue 0.16.9 servers never produce it.
	reasonTopologyPlacementFailed = "TopologyPlacementFailed"

	// reasonPending is the catch-all QuotaReserved=False reason kueue
	// 0.16.x stamps for every not-admitted cause; the message must be
	// inspected to tell a TAS no-fit apart from quota starvation.
	reasonPending = "Pending"

	// The two fragments below identify a TAS no-fit in a kueue 0.16.x
	// condition message. The full message reads e.g.:
	//
	//   couldn't assign flavors to pod set main: topology "block"
	//   doesn't allow to fit any of 16 pod(s)
	//
	// (or "... allows to fit only 8 out of 16 pod(s)"). Prefix built in
	// kueue pkg/scheduler/flavorassigner, topology text in
	// pkg/cache/scheduler/tas_flavor_snapshot.go. Matched as substrings
	// because multi-podset messages are joined with "; " and long
	// messages are truncated at the tail.
	msgCouldntAssignFlavors    = "couldn't assign flavors to pod set"
	msgTopologyNoFitAny        = "doesn't allow to fit"
	msgTopologyNoFitPartial    = "allows to fit only"
	msgInsufficientUnusedQuota = "insufficient unused quota"
	msgKueuePreempting         = "Pending the preemption of"
)

// IsStuckTASNoFit reports whether the Workload is pending specifically
// because no topology domain can fit it — the only condition draining can
// fix. See config.DetectionMode for the kueue version differences behind
// the three modes.
func IsStuckTASNoFit(wl *kueue.Workload, mode config.DetectionMode) bool {
	cond := meta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadQuotaReserved)
	if cond == nil || cond.Status != metav1.ConditionFalse {
		return false
	}
	// A workload being evicted or finished also has QuotaReserved=False;
	// those are handled by their own conditions, not by draining.
	if meta.IsStatusConditionTrue(wl.Status.Conditions, kueue.WorkloadFinished) {
		return false
	}

	byReason := cond.Reason == reasonTopologyPlacementFailed
	byMessage := cond.Reason == reasonPending && messageIsTASNoFit(cond.Message)

	switch mode {
	case config.DetectionReason:
		return byReason
	case config.DetectionMessage:
		return byMessage
	default: // config.DetectionAuto
		return byReason || byMessage
	}
}

// messageIsTASNoFit reports whether a kueue 0.16.x QuotaReserved=False
// message describes a topology no-fit. Every case starts from a flavor
// assignment failure, then names the blocker.
func messageIsTASNoFit(msg string) bool {
	if !strings.Contains(msg, msgCouldntAssignFlavors) {
		return false
	}
	// The topology itself cannot hold the pods (or holds only some).
	if strings.Contains(msg, msgTopologyNoFitAny) || strings.Contains(msg, msgTopologyNoFitPartial) {
		return true
	}
	// Quota exists but is in use. Draining helps only while kueue has not
	// already targeted victims of its own: once it is preempting, the
	// quota frees up without us evicting anyone.
	return strings.Contains(msg, msgInsufficientUnusedQuota) &&
		!strings.Contains(msg, msgKueuePreempting)
}
