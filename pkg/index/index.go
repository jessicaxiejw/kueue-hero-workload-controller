// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

// Package index registers the cache field indexes the controllers list by.
//
// Both controllers answer narrow questions — which nodes carry a drain
// taint, which pods sit on this node, which workloads are stuck — and
// answering them by listing a whole collection and filtering in Go is what
// made this controller expensive: a cache List DeepCopies every object it
// returns, so every such call allocates in proportion to the size of the
// cluster. On the paths that run per reconcile and per watch event that
// dominates the controller's resident memory.
//
// An indexed list returns (and copies) only the matching objects, which in
// steady state — no drain in flight, no stuck hero — is none at all.
//
// Register must run before the controllers start. Every index here is
// derived from fields the cache transforms in pkg/cache keep, and both are
// wired together in cmd/main.go.
package index

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/hero"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/taint"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/victims"
)

const (
	// NodeDrainTainted selects nodes carrying a drain taint attributable
	// to a hero — exactly the nodes taint.FindDrains would keep.
	NodeDrainTainted = "hero.drainTainted"
	// NodeDrainOwner selects the nodes of ONE hero's drain, keyed
	// "<namespace>/<name>".
	NodeDrainOwner = "hero.drainOwner"
	// PodNode selects the pods scheduled on a node.
	PodNode = "spec.nodeName"
	// PodWorkload selects the pods of one Kueue workload, keyed by the
	// workload name from the kueue.x-k8s.io/workload annotation (combine
	// with client.InNamespace for the full key).
	PodWorkload = "hero.podWorkload"
	// WorkloadStuck selects workloads pending on a TAS no-fit.
	WorkloadStuck = "hero.stuck"
	// WorkloadDeactivatedFor selects the victims one drain suspended,
	// keyed by the owning hero "<namespace>/<name>". Declared in
	// pkg/victims beside the annotation it indexes, which pkg/victims
	// lists by directly.
	WorkloadDeactivatedFor = victims.DeactivatedForField
)

// True is the value of the boolean-style indexes above; listing them reads
// client.MatchingFields{index.NodeDrainTainted: index.True}.
const True = "true"

// Register wires every index onto the manager's field indexer. cfg is read
// when an object is indexed, so the indexes follow the configuration the
// manager was started with.
func Register(ctx context.Context, fi client.FieldIndexer, cfg *config.Config) error {
	if err := fi.IndexField(ctx, &corev1.Node{}, NodeDrainTainted, func(obj client.Object) []string {
		if _, ok := taint.Owner(obj.(*corev1.Node), cfg.TaintKey); !ok {
			return nil
		}
		return []string{True}
	}); err != nil {
		return err
	}
	if err := fi.IndexField(ctx, &corev1.Node{}, NodeDrainOwner, func(obj client.Object) []string {
		owner, ok := taint.Owner(obj.(*corev1.Node), cfg.TaintKey)
		if !ok {
			return nil
		}
		return []string{victims.OwnerRef(owner)}
	}); err != nil {
		return err
	}
	if err := fi.IndexField(ctx, &corev1.Pod{}, PodNode, func(obj client.Object) []string {
		node := obj.(*corev1.Pod).Spec.NodeName
		if node == "" {
			return nil // unscheduled; no drain can be waiting on it
		}
		return []string{node}
	}); err != nil {
		return err
	}
	if err := fi.IndexField(ctx, &corev1.Pod{}, PodWorkload, func(obj client.Object) []string {
		wl, ok := obj.(*corev1.Pod).Annotations[kueue.WorkloadAnnotation]
		if !ok {
			return nil
		}
		return []string{wl}
	}); err != nil {
		return err
	}
	if err := fi.IndexField(ctx, &kueue.Workload{}, WorkloadStuck, func(obj client.Object) []string {
		if !hero.IsStuckTASNoFit(obj.(*kueue.Workload), cfg.StuckDetection) {
			return nil
		}
		return []string{True}
	}); err != nil {
		return err
	}
	return fi.IndexField(ctx, &kueue.Workload{}, WorkloadDeactivatedFor, func(obj client.Object) []string {
		owner, ok := obj.(*kueue.Workload).Annotations[victims.DeactivatedForAnnotation]
		if !ok {
			return nil
		}
		return []string{owner}
	})
}
