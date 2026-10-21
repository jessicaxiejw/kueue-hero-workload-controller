// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package index

import (
	"context"
	"fmt"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/taint"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/victims"
)

const (
	ownerRef  = "team-a/hero-1"
	nodeName  = "n1"
	workload  = "hero-1-abc12"
	otherKey  = "example.com/other-taint"
	quotaOnly = `couldn't assign flavors to pod set main: insufficient quota for nvidia.com/gpu in flavor gpu-flavor, request > maximum capacity (24 > 16)`
	tasNoFit  = `couldn't assign flavors to pod set main: topology "cloud.provider.com/topology-block" doesn't allow to fit any of 16 pod(s)`
)

// recordingIndexer captures the extract functions Register wires up so
// each one can be called against a hand-built object. A real manager
// never surfaces a mis-keyed index as an error — it just returns the
// wrong objects — so the functions themselves are what is under test.
type recordingIndexer struct {
	fns map[string]client.IndexerFunc
}

func (r *recordingIndexer) IndexField(_ context.Context, obj client.Object, field string, fn client.IndexerFunc) error {
	key := fmt.Sprintf("%T/%s", obj, field)
	if _, dup := r.fns[key]; dup {
		return fmt.Errorf("index %s registered twice", key)
	}
	r.fns[key] = fn
	return nil
}

// registered runs Register under the default configuration and returns
// the captured extract functions.
func registered(t *testing.T) *recordingIndexer {
	t.Helper()
	cfg := config.Default()
	fi := &recordingIndexer{fns: map[string]client.IndexerFunc{}}
	if err := Register(context.Background(), fi, &cfg); err != nil {
		t.Fatal(err)
	}
	return fi
}

// extract runs the index registered for obj's type under field.
func (r *recordingIndexer) extract(t *testing.T, obj client.Object, field string) []string {
	t.Helper()
	key := fmt.Sprintf("%T/%s", obj, field)
	fn, ok := r.fns[key]
	if !ok {
		t.Fatalf("no index registered for %s", key)
	}
	return fn(obj)
}

func drainNode(effect corev1.TaintEffect, owner string) *corev1.Node {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
	if effect != "" {
		node.Spec.Taints = []corev1.Taint{{
			Key:    config.Default().TaintKey,
			Value:  "hero-cq",
			Effect: effect,
		}}
	}
	if owner != "" {
		node.Annotations = map[string]string{taint.OwnerAnnotation: owner}
	}
	return node
}

func pod(nodeName, wl string) *corev1.Pod {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "team-a"}}
	p.Spec.NodeName = nodeName
	if wl != "" {
		p.Annotations = map[string]string{kueue.WorkloadAnnotation: wl}
	}
	return p
}

func pendingWorkload(message string, finished bool) *kueue.Workload {
	wl := &kueue.Workload{ObjectMeta: metav1.ObjectMeta{Name: workload, Namespace: "team-a"}}
	wl.Status.Conditions = []metav1.Condition{{
		Type:    kueue.WorkloadQuotaReserved,
		Status:  metav1.ConditionFalse,
		Reason:  "Pending",
		Message: message,
	}}
	if finished {
		wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
			Type:   kueue.WorkloadFinished,
			Status: metav1.ConditionTrue,
			Reason: "Succeeded",
		})
	}
	return wl
}

func TestRegisterRegistersEveryIndexOnce(t *testing.T) {
	fi := registered(t)
	if got, want := len(fi.fns), 7; got != want {
		t.Errorf("Register wired %d indexes, want %d", got, want)
	}
}

func TestNodeIndexes(t *testing.T) {
	cases := []struct {
		name           string
		node           *corev1.Node
		wantTainted    []string
		wantDrainOwner []string
	}{
		{
			name:           "our key, NoSchedule, owner annotation",
			node:           drainNode(corev1.TaintEffectNoSchedule, ownerRef),
			wantTainted:    []string{True},
			wantDrainOwner: []string{ownerRef},
		},
		{
			// key+effect is the taint uniqueness pair: the same key
			// under NoExecute was written by something else, and
			// counting it would make drainsInFlight block every new
			// drain on a foreign taint.
			name: "same key under NoExecute is not ours",
			node: drainNode(corev1.TaintEffectNoExecute, ownerRef),
		},
		{
			name: "our taint without an owner annotation is unattributable",
			node: drainNode(corev1.TaintEffectNoSchedule, ""),
		},
		{
			name: "owner annotation without the taint is a leftover",
			node: drainNode("", ownerRef),
		},
		{
			name: "another controller's taint",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        nodeName,
					Annotations: map[string]string{taint.OwnerAnnotation: ownerRef},
				},
				Spec: corev1.NodeSpec{Taints: []corev1.Taint{{
					Key:    otherKey,
					Effect: corev1.TaintEffectNoSchedule,
				}}},
			},
		},
	}

	fi := registered(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := fi.extract(t, tc.node, NodeDrainTainted); !slices.Equal(got, tc.wantTainted) {
				t.Errorf("%s = %v, want %v", NodeDrainTainted, got, tc.wantTainted)
			}
			if got := fi.extract(t, tc.node, NodeDrainOwner); !slices.Equal(got, tc.wantDrainOwner) {
				t.Errorf("%s = %v, want %v", NodeDrainOwner, got, tc.wantDrainOwner)
			}
		})
	}
}

func TestPodIndexes(t *testing.T) {
	cases := []struct {
		name         string
		pod          *corev1.Pod
		wantNode     []string
		wantWorkload []string
	}{
		{
			name:         "scheduled pod of a kueue workload",
			pod:          pod(nodeName, workload),
			wantNode:     []string{nodeName},
			wantWorkload: []string{workload},
		},
		{
			name:         "unscheduled pod",
			pod:          pod("", workload),
			wantWorkload: []string{workload},
		},
		{
			name:     "pod not managed by kueue",
			pod:      pod(nodeName, ""),
			wantNode: []string{nodeName},
		},
	}

	fi := registered(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := fi.extract(t, tc.pod, PodNode); !slices.Equal(got, tc.wantNode) {
				t.Errorf("%s = %v, want %v", PodNode, got, tc.wantNode)
			}
			if got := fi.extract(t, tc.pod, PodWorkload); !slices.Equal(got, tc.wantWorkload) {
				t.Errorf("%s = %v, want %v", PodWorkload, got, tc.wantWorkload)
			}
		})
	}
}

func TestWorkloadStuckIndex(t *testing.T) {
	cases := []struct {
		name string
		wl   *kueue.Workload
		want []string
	}{
		{
			name: "pending on a TAS no-fit",
			wl:   pendingWorkload(tasNoFit, false),
			want: []string{True},
		},
		{
			name: "pending on quota, which draining cannot fix",
			wl:   pendingWorkload(quotaOnly, false),
		},
		{
			name: "finished workload with a stale pending condition",
			wl:   pendingWorkload(tasNoFit, true),
		},
	}

	fi := registered(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := fi.extract(t, tc.wl, WorkloadStuck); !slices.Equal(got, tc.want) {
				t.Errorf("%s = %v, want %v", WorkloadStuck, got, tc.want)
			}
		})
	}
}

func TestWorkloadDeactivatedForIndex(t *testing.T) {
	marked := &kueue.Workload{ObjectMeta: metav1.ObjectMeta{
		Name:        "victim",
		Namespace:   "team-b",
		Annotations: map[string]string{victims.DeactivatedForAnnotation: ownerRef},
	}}
	unmarked := &kueue.Workload{ObjectMeta: metav1.ObjectMeta{Name: "victim", Namespace: "team-b"}}

	fi := registered(t)
	if got, want := fi.extract(t, marked, WorkloadDeactivatedFor), []string{ownerRef}; !slices.Equal(got, want) {
		t.Errorf("%s on a marked victim = %v, want %v", WorkloadDeactivatedFor, got, want)
	}
	if got := fi.extract(t, unmarked, WorkloadDeactivatedFor); got != nil {
		t.Errorf("%s on an unmarked workload = %v, want nil", WorkloadDeactivatedFor, got)
	}
}

func TestWorkloadAdmittedClusterQueueIndex(t *testing.T) {
	const cq = "admitted-cq"
	admitted := func(finished bool) *kueue.Workload {
		wl := pendingWorkload("", finished)
		wl.Status.Admission = &kueue.Admission{ClusterQueue: cq}
		return wl
	}
	cases := []struct {
		name string
		wl   *kueue.Workload
		want []string
	}{
		{name: "admitted workload holds its CQ's quota", wl: admitted(false), want: []string{cq}},
		{name: "pending workload holds no quota", wl: pendingWorkload(tasNoFit, false)},
		{name: "finished workload released its quota", wl: admitted(true)},
	}

	fi := registered(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := fi.extract(t, tc.wl, WorkloadAdmittedClusterQueue); !slices.Equal(got, tc.want) {
				t.Errorf("%s = %v, want %v", WorkloadAdmittedClusterQueue, got, tc.want)
			}
		})
	}
}
