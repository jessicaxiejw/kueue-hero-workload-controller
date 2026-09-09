// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package hero

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
)

func pendingCondition(reason, message string) metav1.Condition {
	return metav1.Condition{
		Type:    kueue.WorkloadQuotaReserved,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: message,
	}
}

func TestIsStuckTASNoFit(t *testing.T) {
	cases := []struct {
		name string
		wl   *kueue.Workload
		want map[config.DetectionMode]bool
	}{
		{
			name: "0.16 TAS no-fit, all pods",
			wl:   heroWorkload().Condition(pendingCondition("Pending", msg016NoFitAll)).Obj(),
			want: map[config.DetectionMode]bool{
				config.DetectionAuto:    true,
				config.DetectionMessage: true,
				config.DetectionReason:  false,
			},
		},
		{
			name: "0.16 TAS no-fit, partial fit",
			wl:   heroWorkload().Condition(pendingCondition("Pending", msg016NoFitPartial)).Obj(),
			want: map[config.DetectionMode]bool{
				config.DetectionAuto:    true,
				config.DetectionMessage: true,
				config.DetectionReason:  false,
			},
		},
		{
			name: "0.16 multi-podset message with one TAS failure",
			wl:   heroWorkload().Condition(pendingCondition("Pending", msg016MultiPodSet)).Obj(),
			want: map[config.DetectionMode]bool{
				config.DetectionAuto:    true,
				config.DetectionMessage: true,
			},
		},
		{
			name: "0.16 quota-only pending must NOT match",
			wl:   heroWorkload().Condition(pendingCondition("Pending", msg016QuotaOnly)).Obj(),
			want: map[config.DetectionMode]bool{
				config.DetectionAuto:    false,
				config.DetectionMessage: false,
				config.DetectionReason:  false,
			},
		},
		{
			name: "0.19+ granular reason",
			wl:   heroWorkload().Condition(pendingCondition("TopologyPlacementFailed", `couldn't assign flavors to pod set main: topology "block" doesn't allow to fit any of 16 pod(s)`)).Obj(),
			want: map[config.DetectionMode]bool{
				config.DetectionAuto:   true,
				config.DetectionReason: true,
				// Message mode still matches: the message text is the same.
				config.DetectionMessage: false,
			},
		},
		{
			name: "0.19+ granular quota reason must NOT match",
			wl:   heroWorkload().Condition(pendingCondition("WaitingForQuota", "waiting for quota")).Obj(),
			want: map[config.DetectionMode]bool{
				config.DetectionAuto:    false,
				config.DetectionMessage: false,
				config.DetectionReason:  false,
			},
		},
		{
			name: "no QuotaReserved condition at all",
			wl:   heroWorkload().Obj(),
			want: map[config.DetectionMode]bool{
				config.DetectionAuto: false,
			},
		},
		{
			name: "QuotaReserved true (admitted) must NOT match",
			wl: heroWorkload().Condition(metav1.Condition{
				Type:   kueue.WorkloadQuotaReserved,
				Status: metav1.ConditionTrue,
				Reason: "QuotaReserved",
			}).Obj(),
			want: map[config.DetectionMode]bool{
				config.DetectionAuto: false,
			},
		},
		{
			name: "finished workload must NOT match even with stale pending condition",
			wl: heroWorkload().
				Condition(pendingCondition("Pending", msg016NoFitAll)).
				Condition(metav1.Condition{
					Type:   kueue.WorkloadFinished,
					Status: metav1.ConditionTrue,
					Reason: "Succeeded",
				}).Obj(),
			want: map[config.DetectionMode]bool{
				config.DetectionAuto: false,
			},
		},
		{
			// Slice no-fit uses unit "slice" in the same message template
			// (kueue notFitMessage: unit is "pod" only when sliceSize==1).
			name: "0.16 slice no-fit message matches",
			wl: heroWorkload().Condition(pendingCondition("Pending",
				`couldn't assign flavors to pod set workers: topology "cloud.provider.com/topology-rack" doesn't allow to fit any of 6 slice(s)`)).Obj(),
			want: map[config.DetectionMode]bool{
				config.DetectionAuto:    true,
				config.DetectionMessage: true,
			},
		},
		{
			// TAS level mismatch reported together with a quota
			// shortfall in the same pod set message.
			name: "0.16 missing topology level with unused quota shortfall matches",
			wl: heroWorkload().Condition(pendingCondition("Pending",
				`couldn't assign flavors to pod set worker: Flavor "cpu-flavor" does not contain the requested level, insufficient unused quota for nvidia.com/gpu in flavor gpu-flavor, 4 more needed`)).Obj(),
			want: map[config.DetectionMode]bool{
				config.DetectionAuto:    true,
				config.DetectionMessage: true,
				config.DetectionReason:  false,
			},
		},
		{
			name: "truncated message tail still matches",
			wl: heroWorkload().Condition(pendingCondition("Pending",
				`couldn't assign flavors to pod set main: topology "cloud.provider.com/topology-block" doesn't allow to fit any of 16 pod(s). Total nodes: 4; excl`)).Obj(),
			want: map[config.DetectionMode]bool{
				config.DetectionAuto: true,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for mode, want := range tc.want {
				if got := IsStuckTASNoFit(tc.wl, mode); got != want {
					t.Errorf("IsStuckTASNoFit(mode=%s) = %v, want %v", mode, got, want)
				}
			}
		})
	}
}

func TestIsStuckTASNoFitMessageModeIgnoresGranularReason(t *testing.T) {
	// In message mode a 0.19+ workload (granular reason, same message
	// text) must not match: the operator pinned legacy behavior.
	wl := heroWorkload().Condition(pendingCondition("TopologyPlacementFailed", msg016NoFitAll)).Obj()
	if IsStuckTASNoFit(wl, config.DetectionMessage) {
		t.Error("message mode matched a granular-reason condition; should require reason Pending")
	}
}
