// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

// Package victims owns the suspend/reactivate marker on victim Workloads,
// shared by the drain controller (which suspends and normally reactivates)
// and the janitor (which sweeps at drain teardown).
package victims

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/metrics"
)

// DeactivatedForAnnotation marks a victim Workload the drain controller
// suspended, naming the hero drain that owns the suspension
// ("<namespace>/<name>"). It is how reactivation finds exactly the victims
// we deactivated — crash-safe because it lives on the victim itself.
const DeactivatedForAnnotation = "hero.coreweave.com/deactivated-for"

// DeactivatedForField is the cache field index over
// DeactivatedForAnnotation, registered by pkg/index. It lives here beside
// the annotation it indexes, and because pkg/index imports this package.
const DeactivatedForField = "hero.deactivatedFor"

// EventReactivated is recorded on a victim when it is handed back.
const EventReactivated = "ReactivatedAfterHeroDrain"

// OwnerRef is the annotation value for a hero.
func OwnerRef(owner types.NamespacedName) string {
	return owner.Namespace + "/" + owner.Name
}

// Reactivate unpauses one victim and clears the marker in the same patch,
// so the marker never leaks into future logic.
func Reactivate(ctx context.Context, c client.Client, recorder record.EventRecorder, victim *kueue.Workload) error {
	patch := client.MergeFrom(victim.DeepCopy())
	active := true
	victim.Spec.Active = &active
	delete(victim.Annotations, DeactivatedForAnnotation)
	if err := c.Patch(ctx, victim, patch); err != nil {
		return err
	}
	metrics.VictimsReactivated.Inc()
	// No hero identity here: the hero may live in another namespace, and
	// events must not leak foreign workload names (its UID is on the
	// victim's suspension event).
	recorder.Event(victim, corev1.EventTypeNormal, EventReactivated,
		"reactivated after a hero drain ended; kueue will requeue it outside the drained domain")
	return nil
}

// SweepFor unconditionally reactivates every workload the given hero's
// drain suspended — without waiting for kueue's eviction to complete. A
// suspension kueue never executed is cancelled (the workload just keeps
// running); an executed one requeues normally. Called at drain teardown so
// no victim stays suspended or marked past its drain's end. Returns how
// many victims were swept.
func SweepFor(ctx context.Context, c client.Client, recorder record.EventRecorder, owner types.NamespacedName) (int, error) {
	ours := &kueue.WorkloadList{}
	if err := c.List(ctx, ours, client.MatchingFields{DeactivatedForField: OwnerRef(owner)}); err != nil {
		return 0, err
	}
	swept := 0
	for i := range ours.Items {
		if err := Reactivate(ctx, c, recorder, &ours.Items[i]); err != nil {
			return swept, err
		}
		swept++
	}
	return swept, nil
}
