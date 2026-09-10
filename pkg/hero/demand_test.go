// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package hero

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
)

func TestGPURequest(t *testing.T) {
	cases := []struct {
		name string
		wl   *kueue.Workload
		want int64
	}{
		{
			name: "single podset requests",
			wl:   heroWorkload().Obj(), // 16 pods × 8 GPUs
			want: 128,
		},
		{
			name: "multi podset",
			wl: utiltesting.MakeWorkload("multi", "ns").PodSets(
				*utiltesting.MakePodSet("leader", 1).Request("nvidia.com/gpu", "1").Obj(),
				*utiltesting.MakePodSet("workers", 4).Request("nvidia.com/gpu", "8").Obj(),
			).Obj(),
			want: 33,
		},
		{
			name: "limits-only falls back",
			wl: utiltesting.MakeWorkload("limits", "ns").PodSets(
				*utiltesting.MakePodSet("main", 2).Limit("nvidia.com/gpu", "4").Obj(),
			).Obj(),
			want: 8,
		},
		{
			name: "no GPUs",
			wl: utiltesting.MakeWorkload("cpu", "ns").PodSets(
				*utiltesting.MakePodSet("main", 10).Request(corev1.ResourceCPU, "4").Obj(),
			).Obj(),
			want: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := GPURequest(tc.wl, "nvidia.com/gpu")
			if got.Value() != tc.want {
				t.Errorf("GPURequest = %s, want %d", got.String(), tc.want)
			}
		})
	}
}

func TestDemand(t *testing.T) {
	type want struct {
		size  int64
		count int
	}
	cases := []struct {
		name string
		wl   *kueue.Workload
		want []want
	}{
		{
			name: "slice pair: chunk per slice",
			wl: utiltesting.MakeWorkload("slices", "ns").PodSets(
				*utiltesting.MakePodSet("workers", 12).
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(4).
					Request("nvidia.com/gpu", "8").Obj(),
			).Obj(),
			want: []want{{32, 3}},
		},
		{
			name: "slice count rounds up on partial slice",
			wl: utiltesting.MakeWorkload("partial-slice", "ns").PodSets(
				*utiltesting.MakePodSet("workers", 10).
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(4).
					Request("nvidia.com/gpu", "8").Obj(),
			).Obj(),
			want: []want{{32, 3}},
		},
		{
			// Plain required contributes NO demand: not a drain trigger.
			name: "plain required contributes nothing",
			wl: utiltesting.MakeWorkload("plain-required", "ns").PodSets(
				*utiltesting.MakePodSet("main", 16).
					RequiredTopologyRequest(levelBlock).
					Request("nvidia.com/gpu", "8").Obj(),
			).Obj(),
			want: nil,
		},
		{
			// Two slice podsets with equal slice sizes merge counts.
			name: "two slice podsets merge equal chunks",
			wl: utiltesting.MakeWorkload("two-slices", "ns").PodSets(
				*utiltesting.MakePodSet("a", 4).
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(2).
					Request("nvidia.com/gpu", "8").Obj(),
				*utiltesting.MakePodSet("b", 4).
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(2).
					Request("nvidia.com/gpu", "8").Obj(),
			).Obj(),
			want: []want{{16, 4}},
		},
		{
			name: "no trigger podsets: no demand",
			wl: utiltesting.MakeWorkload("plain", "ns").PodSets(
				*utiltesting.MakePodSet("main", 4).
					Request("nvidia.com/gpu", "8").Obj(),
			).Obj(),
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Demand(tc.wl, "nvidia.com/gpu")
			if len(got) != len(tc.want) {
				t.Fatalf("Demand = %+v, want %d chunks %+v", got, len(tc.want), tc.want)
			}
			for i := range got {
				if got[i].Size.Value() != tc.want[i].size || got[i].Count != tc.want[i].count {
					t.Errorf("chunk %d = (%s, %d), want (%d, %d)",
						i, got[i].Size.String(), got[i].Count, tc.want[i].size, tc.want[i].count)
				}
			}
		})
	}
}

func TestPodCountAndPriority(t *testing.T) {
	wl := heroWorkload().Obj()
	if n := PodCount(wl); n != 16 {
		t.Errorf("PodCount = %d, want 16", n)
	}
	if p := Priority(wl); p != 1000 {
		t.Errorf("Priority = %d, want 1000", p)
	}
	if p := Priority(utiltesting.MakeWorkload("noprio", "ns").Obj()); p != 0 {
		t.Errorf("Priority without spec.priority = %d, want 0", p)
	}
}
