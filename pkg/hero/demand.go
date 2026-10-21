// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package hero

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// Chunk is one unit of the hero's drain demand: Count units of Size
// contiguous GPU capacity, each unit needing to land whole inside a single
// domain at the drain level.
type Chunk struct {
	Size  resource.Quantity
	Count int
}

// Demand translates the hero's slice topology requirements into drain
// demand: the list of contiguous-capacity chunks the drained domain set
// must supply. Each slice podset (slice-required + slice-size, the only
// drain trigger) contributes ceil(pods/sliceSize) chunks of one slice's
// GPU request each; the selector greedily packs chunks into as few
// domains as possible, all within one parent domain.
//
// Podsets without the slice pair contribute nothing. Equal-size chunks
// are merged by count. Nil result = no drain demand.
func Demand(wl *kueue.Workload, gpu corev1.ResourceName) []Chunk {
	var chunks []Chunk
	for i := range wl.Spec.PodSets {
		ps := &wl.Spec.PodSets[i]
		tr := ps.TopologyRequest
		if tr == nil {
			continue
		}
		if tr.PodSetSliceRequiredTopology == nil || *tr.PodSetSliceRequiredTopology == "" ||
			tr.PodSetSliceSize == nil || *tr.PodSetSliceSize <= 0 {
			continue
		}
		sliceSize := *tr.PodSetSliceSize
		perPod := podGPURequestOf(ps, gpu)
		size := resource.Quantity{}
		for range sliceSize {
			size.Add(perPod)
		}
		count := int((ps.Count + sliceSize - 1) / sliceSize) // ceil
		chunks = appendChunk(chunks, size, count)
	}
	return chunks
}

// appendChunk merges equal-size chunks.
func appendChunk(chunks []Chunk, size resource.Quantity, count int) []Chunk {
	if size.IsZero() || count <= 0 {
		return chunks
	}
	for i := range chunks {
		if chunks[i].Size.Cmp(size) == 0 {
			chunks[i].Count += count
			return chunks
		}
	}
	return append(chunks, Chunk{Size: size, Count: count})
}

// podSetGPURequest is per-pod request × pod count for one podset.
func podSetGPURequest(ps *kueue.PodSet, gpu corev1.ResourceName) resource.Quantity {
	perPod := podGPURequestOf(ps, gpu)
	total := resource.Quantity{}
	for range ps.Count {
		total.Add(perPod)
	}
	return total
}

// podGPURequestOf sums one pod's GPU request across containers; requests
// default to limits when omitted, mirroring kueue's own defaulting.
func podGPURequestOf(ps *kueue.PodSet, gpu corev1.ResourceName) resource.Quantity {
	perPod := resource.Quantity{}
	for j := range ps.Template.Spec.Containers {
		c := &ps.Template.Spec.Containers[j]
		if q, ok := c.Resources.Requests[gpu]; ok {
			perPod.Add(q)
		} else if q, ok := c.Resources.Limits[gpu]; ok {
			perPod.Add(q)
		}
	}
	return perPod
}

// GPURequest sums the workload's total GPU request across podsets.
func GPURequest(wl *kueue.Workload, gpu corev1.ResourceName) resource.Quantity {
	total := resource.Quantity{}
	for i := range wl.Spec.PodSets {
		total.Add(podSetGPURequest(&wl.Spec.PodSets[i], gpu))
	}
	return total
}

// AdmittedGPUUsage is the GPU quota the workload currently holds in its
// admitted ClusterQueue: per podset assignment, kueue's recorded
// resourceUsage when present, else the podset's per-pod request times the
// admitted count (falling back to the spec count). Zero when not admitted.
func AdmittedGPUUsage(wl *kueue.Workload, gpu corev1.ResourceName) resource.Quantity {
	total := resource.Quantity{}
	if wl.Status.Admission == nil {
		return total
	}
	for i := range wl.Status.Admission.PodSetAssignments {
		psa := &wl.Status.Admission.PodSetAssignments[i]
		if q, ok := psa.ResourceUsage[gpu]; ok {
			total.Add(q)
			continue
		}
		for j := range wl.Spec.PodSets {
			ps := &wl.Spec.PodSets[j]
			if ps.Name != psa.Name {
				continue
			}
			count := ps.Count
			if psa.Count != nil {
				count = *psa.Count
			}
			perPod := podGPURequestOf(ps, gpu)
			for range count {
				total.Add(perPod)
			}
		}
	}
	return total
}

// PodCount is the workload's total pod count across podsets (the
// heroPodCount term of the cost normalization).
func PodCount(wl *kueue.Workload) int32 {
	var n int32
	for i := range wl.Spec.PodSets {
		n += wl.Spec.PodSets[i].Count
	}
	return n
}

// Priority returns the kueue-resolved priority value from spec.priority
// (populated by kueue from the WorkloadPriorityClass at admission time);
// 0 when unset.
func Priority(wl *kueue.Workload) int32 {
	if wl.Spec.Priority != nil {
		return *wl.Spec.Priority
	}
	return 0
}
