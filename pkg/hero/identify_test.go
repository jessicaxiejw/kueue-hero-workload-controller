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

func TestIsHero(t *testing.T) {
	cfg := testConfig()

	cases := []struct {
		name       string
		wl         *kueue.Workload
		cq         *kueue.ClusterQueue
		want       bool
		wantReason NotHeroReason
	}{
		{
			name: "all identifiers match",
			wl:   heroWorkload().Obj(),
			cq:   heroCQ(),
			want: true,
		},
		{
			name:       "nil ClusterQueue",
			wl:         heroWorkload().Obj(),
			cq:         nil,
			want:       false,
			wantReason: ReasonCQNotHero,
		},
		{
			name:       "ClusterQueue not hero-enabled",
			wl:         heroWorkload().Obj(),
			cq:         utiltesting.MakeClusterQueue("plain-cq").Obj(),
			want:       false,
			wantReason: ReasonCQNotHero,
		},
		{
			name:       "ClusterQueue label false",
			wl:         heroWorkload().Obj(),
			cq:         utiltesting.MakeClusterQueue("off-cq").Label(cfg.HeroCQLabelKey, "false").Obj(),
			want:       false,
			wantReason: ReasonCQNotHero,
		},
		{
			name:       "no priorityClassRef",
			wl:         heroWorkload().PriorityClassRef(nil).Obj(),
			cq:         heroCQ(),
			want:       false,
			wantReason: ReasonNoPriorityClassRef,
		},
		{
			name:       "wrong priority class name",
			wl:         heroWorkload().WorkloadPriorityClassRef("merely-important").Obj(),
			cq:         heroCQ(),
			want:       false,
			wantReason: ReasonWrongPriorityClass,
		},
		{
			name:       "pod PriorityClass kind instead of WorkloadPriorityClass",
			wl:         heroWorkload().PodPriorityClassRef(cfg.HeroPriorityClassName).Obj(),
			cq:         heroCQ(),
			want:       false,
			wantReason: ReasonWrongPriorityClass,
		},
		{
			name: "missing taint toleration on slice podset",
			wl: heroWorkload().PodSets(*slicePodSet("main", 16).
				Request("nvidia.com/gpu", "8").
				Obj()).Obj(),
			cq:         heroCQ(),
			want:       false,
			wantReason: ReasonMissingToleration,
		},
		{
			name: "one of two slice podsets missing toleration",
			wl: heroWorkload().PodSets(
				*slicePodSet("leader", 1).
					Toleration(heroToleration(cfg.TaintKey)).Obj(),
				*slicePodSet("workers", 15).Obj(),
			).Obj(),
			cq:         heroCQ(),
			want:       false,
			wantReason: ReasonMissingToleration,
		},
		{
			// Only slice-pair podsets can land in a drained domain, so
			// the untainted sidecars need no toleration.
			name: "non-slice podsets need no toleration",
			wl: heroWorkload().PodSets(
				*slicePodSet("workers", 15).
					Toleration(heroToleration(cfg.TaintKey)).Obj(),
				*utiltesting.MakePodSet("leader", 1).Obj(),
				*utiltesting.MakePodSet("sidecar", 1).
					RequiredTopologyRequest(levelBlock).Obj(),
			).Obj(),
			cq:   heroCQ(),
			want: true,
		},
		{
			// No slice pair anywhere: nothing to drain for. Admitting it
			// to the drain queue would head-of-line block real heroes,
			// so it is not a hero.
			name: "no slice podsets at all",
			wl: heroWorkload().PodSets(*utiltesting.MakePodSet("main", 16).
				RequiredTopologyRequest(levelBlock).
				Toleration(heroToleration(cfg.TaintKey)).Obj()).Obj(),
			cq:         heroCQ(),
			want:       false,
			wantReason: ReasonNoSliceTopology,
		},
		{
			// slice-required without slice-size is not the slice pair.
			name: "slice topology without slice size",
			wl: heroWorkload().PodSets(*utiltesting.MakePodSet("main", 16).
				SliceRequiredTopologyRequest(levelBlock).
				Toleration(heroToleration(cfg.TaintKey)).Obj()).Obj(),
			cq:         heroCQ(),
			want:       false,
			wantReason: ReasonNoSliceTopology,
		},
		{
			name: "no topology request at all",
			wl: heroWorkload().PodSets(*utiltesting.MakePodSet("main", 16).
				Toleration(heroToleration(cfg.TaintKey)).Obj()).Obj(),
			cq:         heroCQ(),
			want:       false,
			wantReason: ReasonNoSliceTopology,
		},
		{
			name: "toleration via Exists-all wildcard",
			wl: heroWorkload().PodSets(*slicePodSet("main", 16).
				Toleration(corev1.Toleration{Operator: corev1.TolerationOpExists}).
				Obj()).Obj(),
			cq:   heroCQ(),
			want: true,
		},
		{
			name: "toleration with NoSchedule effect explicit",
			wl: heroWorkload().PodSets(*slicePodSet("main", 16).
				Toleration(corev1.Toleration{
					Key:      cfg.TaintKey,
					Operator: corev1.TolerationOpExists,
					Effect:   corev1.TaintEffectNoSchedule,
				}).
				Obj()).Obj(),
			cq:   heroCQ(),
			want: true,
		},
		{
			name: "toleration for different key",
			wl: heroWorkload().PodSets(*slicePodSet("main", 16).
				Toleration(heroToleration("some.other.com/taint")).
				Obj()).Obj(),
			cq:         heroCQ(),
			want:       false,
			wantReason: ReasonMissingToleration,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := IsHero(tc.wl, tc.cq, cfg)
			if got != tc.want {
				t.Errorf("IsHero() = %v, want %v (reason %q)", got, tc.want, reason)
			}
			if !tc.want && reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}
