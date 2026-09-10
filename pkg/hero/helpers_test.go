// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package hero

import (
	corev1 "k8s.io/api/core/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
)

// Real condition messages produced by kueue 0.16.9 (see
// pkg/cache/scheduler/tas_cache_test.go and flavorassigner at that tag).
const (
	msg016NoFitAll     = `couldn't assign flavors to pod set main: topology "cloud.provider.com/topology-block" doesn't allow to fit any of 16 pod(s). Total nodes: 4; excluded: resource "nvidia.com/gpu": 4`
	msg016NoFitPartial = `couldn't assign flavors to pod set main: topology "cloud.provider.com/topology-block" allows to fit only 8 out of 16 pod(s)`
	msg016MultiPodSet  = `couldn't assign flavors to pod set leader: topology "cloud.provider.com/topology-block" doesn't allow to fit any of 1 pod(s); couldn't assign flavors to pod set workers: insufficient quota for nvidia.com/gpu`
	msg016QuotaOnly    = `couldn't assign flavors to pod set main: insufficient quota for nvidia.com/gpu in flavor gpu-flavor, request > maximum capacity (24 > 16)`
	// Kueue appends "Pending the preemption of N workload(s)" when it has
	// already targeted victims for the unused-quota shortfall: the quota
	// is about to free up on its own, so draining must stay out of it.
	msg016PreemptionPending = `couldn't assign flavors to pod set main: insufficient unused quota for nvidia.com/gpu in flavor gpu-flavor, 8 more needed. Pending the preemption of 2 workload(s)`
)

func testConfig() *config.Config {
	cfg := config.Default()
	return &cfg
}

func heroToleration(key string) corev1.Toleration {
	return corev1.Toleration{Key: key, Operator: corev1.TolerationOpExists}
}

// slicePodSet builds a drain-relevant podset: one carrying the slice pair
// (podset-slice-required-topology + podset-slice-size), the only shape
// that must tolerate the drain taint.
func slicePodSet(name kueue.PodSetReference, count int) *utiltesting.PodSetWrapper {
	return utiltesting.MakePodSet(name, count).
		SliceRequiredTopologyRequest(levelBlock).
		SliceSizeTopologyRequest(8)
}

// heroWorkload builds a workload passing all three hero identifiers under
// the default config.
func heroWorkload() *utiltesting.WorkloadWrapper {
	cfg := config.Default()
	return utiltesting.MakeWorkload("hero", "team-a").
		WorkloadPriorityClassRef(cfg.HeroPriorityClassName).
		Priority(1000).
		PodSets(*slicePodSet("main", 16).
			Request("nvidia.com/gpu", "8").
			Toleration(heroToleration(cfg.TaintKey)).
			Obj())
}

func heroCQ() *kueue.ClusterQueue {
	cfg := config.Default()
	return utiltesting.MakeClusterQueue("hero-cq").Label(cfg.HeroCQLabelKey, "true").Obj()
}

const (
	levelBlock = "cloud.provider.com/topology-block"
	levelRack  = "cloud.provider.com/topology-rack"
	levelHost  = "kubernetes.io/hostname"
)
