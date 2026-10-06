// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package cache

import (
	"cmp"
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
	testingpod "sigs.k8s.io/kueue/pkg/util/testingjobs/pod"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/hero"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/selection"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/snapshot"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/taint"
)

// The transforms are a field inventory: they keep what the controller's
// readers touch and drop the rest. A field dropped here but read somewhere
// reads as its zero value — no error, no panic, wrong answers in
// production only.
//
// The tests below are that inventory's tripwire. Each one builds one fat
// fixture per cached type — every field an API server puts on the object,
// not just the ones the controller wants — runs the real readers over the
// fixture and over its transformed copy, and demands identical results. A
// field read added anywhere in the controller but not added here fails
// here first. The companion tests assert what the strip removes, so the
// transforms cannot quietly stop stripping either.

const (
	levelBlock = "cloud.provider.com/topology-block"
	levelRack  = "cloud.provider.com/topology-rack"
	gpu        = corev1.ResourceName("nvidia.com/gpu")
	heroCQName = "hero-cq"

	// A real kueue 0.16.9 QuotaReserved=False message for a TAS no-fit.
	msgTASNoFit = `couldn't assign flavors to pod set workers: topology "cloud.provider.com/topology-block" doesn't allow to fit any of 16 pod(s)`

	// Fixture junk: values the fixtures carry only to be stripped.
	appTrainer   = "trainer"
	imageTrainer = "trainer:v2"
	imageInit    = "busybox:1.36"
	volumeName   = "scratch"
)

var (
	heroKey  = types.NamespacedName{Namespace: "team-a", Name: "hero"}
	otherKey = types.NamespacedName{Namespace: "team-b", Name: "other-hero"}
	now      = time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)

	topologyLevels = []string{levelBlock, levelRack, corev1.LabelHostname}

	drainTaint = corev1.Taint{
		Key:    config.Default().TaintKey,
		Value:  heroCQName,
		Effect: corev1.TaintEffectNoSchedule,
	}
)

func testCfg() *config.Config {
	cfg := config.Default()
	cfg.NonBlockingPodLabels = map[string]string{"app": "hpc-verification"}
	return &cfg
}

func TestTransformPodKeepsWhatItsReadersRead(t *testing.T) {
	cfg := testCfg()
	nodes := fatNodes()
	pods := fatPods()

	want := blockSnapshot(cfg, nodes, pods)
	got := blockSnapshot(cfg, nodes, transformPods(t, cfg, pods))

	// Guard the fixture: a reader that sees nothing on either side
	// compares equal and proves nothing.
	if b1 := want.Domains["b1"]; len(want.Domains) != 2 || len(b1.Victims) < 2 || b1.NonReclaimableGPU.IsZero() {
		t.Fatalf("fixture exercises too little of Build: %s", render(t, want))
	}
	if g, w := render(t, got), render(t, want); g != w {
		t.Errorf("snapshot differs after the pod transform\ngot:  %s\nwant: %s", g, w)
	}
}

func TestTransformNodeKeepsWhatItsReadersRead(t *testing.T) {
	cfg := testCfg()
	nodes := fatNodes()
	pods := fatPods()

	want := readNodes(cfg, nodes, pods)
	got := readNodes(cfg, transformNodes(t, nodes), pods)

	if len(want.Owners) != 2 || len(want.Drains) != 2 || want.Blocks.Domains["b1"].AllocatableGPU.IsZero() {
		t.Fatalf("fixture exercises too little of the node readers: %s", render(t, want))
	}
	if g, w := render(t, got), render(t, want); g != w {
		t.Errorf("node reads differ after the node transform\ngot:  %s\nwant: %s", g, w)
	}
}

func TestTransformWorkloadKeepsWhatItsReadersRead(t *testing.T) {
	cfg := testCfg()
	wl := fatWorkload()
	nodes := fatNodes()

	want := readWorkload(cfg, wl, nodes)
	got := readWorkload(cfg, transformWorkload(t, wl), nodes)

	if !want.IsHero || len(want.Demand) == 0 || want.GroupingLevel == "" ||
		!want.Stuck[string(config.DetectionAuto)] || len(want.Victims) == 0 || len(want.HeroDomains) == 0 {
		t.Fatalf("fixture exercises too little of the workload readers: %s", render(t, want))
	}
	if g, w := render(t, got), render(t, want); g != w {
		t.Errorf("workload reads differ after the workload transform\ngot:  %s\nwant: %s", g, w)
	}
}

func TestTransformPodDropsTheRest(t *testing.T) {
	cfg := testCfg()
	pod := fatVictimPod()
	got := transformPod(t, cfg, pod)

	if got.ManagedFields != nil {
		t.Errorf("managedFields survived: %v", got.ManagedFields)
	}
	if got.OwnerReferences != nil || got.Finalizers != nil {
		t.Errorf("ownerReferences %v / finalizers %v survived", got.OwnerReferences, got.Finalizers)
	}
	if want := map[string]string{kueue.WorkloadAnnotation: "wl-a"}; !reflect.DeepEqual(got.Annotations, want) {
		t.Errorf("annotations = %v, want only %s", got.Annotations, kueue.WorkloadAnnotation)
	}
	// Labels stay: classify matches NonBlockingPodLabels against them.
	if !reflect.DeepEqual(got.Labels, pod.Labels) {
		t.Errorf("labels = %v, want %v", got.Labels, pod.Labels)
	}
	if want := (corev1.PodStatus{Phase: corev1.PodRunning}); !reflect.DeepEqual(got.Status, want) {
		t.Errorf("status = %+v, want the phase alone", got.Status)
	}
	// Reusing got's own containers leaves every OTHER spec field compared
	// against its zero value.
	if want := (corev1.PodSpec{NodeName: "n1", Containers: got.Spec.Containers}); !reflect.DeepEqual(got.Spec, want) {
		t.Errorf("spec keeps more than the node name and the containers: %+v", got.Spec)
	}
	// Two of the victim's three containers request GPUs; the CPU-only
	// sidecar and the GPU-requesting init container are gone, and what is
	// left holds nothing but the GPU amounts.
	want := []corev1.Container{
		{Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{gpu: resource.MustParse("4")},
			Limits:   corev1.ResourceList{gpu: resource.MustParse("4")},
		}},
		{Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{gpu: resource.MustParse("2")},
		}},
	}
	if !reflect.DeepEqual(got.Spec.Containers, want) {
		t.Errorf("containers = %+v, want %+v", got.Spec.Containers, want)
	}
}

func TestTransformNodeKeepsMetadataAndSpecVerbatim(t *testing.T) {
	node := fatDrainedNode()
	got := transformNode(t, node)

	// pkg/taint writes nodes back with Get+Update, so anything dropped
	// from a cached node's metadata or spec would be dropped from the node
	// itself on the next taint write.
	if !reflect.DeepEqual(got.ObjectMeta, node.ObjectMeta) {
		t.Errorf("metadata changed\ngot:  %+v\nwant: %+v", got.ObjectMeta, node.ObjectMeta)
	}
	if !reflect.DeepEqual(got.Spec, node.Spec) {
		t.Errorf("spec changed\ngot:  %+v\nwant: %+v", got.Spec, node.Spec)
	}
	want := corev1.NodeStatus{
		Allocatable: node.Status.Allocatable,
		Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
	}
	if !reflect.DeepEqual(got.Status, want) {
		t.Errorf("status = %+v, want the allocatable and the Ready condition alone", got.Status)
	}
}

func TestTransformWorkloadDropsPodTemplates(t *testing.T) {
	wl := fatWorkload()
	got := transformWorkload(t, wl)

	if got.ManagedFields != nil || got.OwnerReferences != nil {
		t.Errorf("managedFields %v / ownerReferences %v survived", got.ManagedFields, got.OwnerReferences)
	}
	wantMeta := *wl.ObjectMeta.DeepCopy()
	wantMeta.ManagedFields = nil
	wantMeta.OwnerReferences = nil
	if !reflect.DeepEqual(got.ObjectMeta, wantMeta) {
		t.Errorf("metadata beyond managedFields and ownerReferences changed\ngot:  %+v\nwant: %+v", got.ObjectMeta, wantMeta)
	}
	// Status is untouched: every workload write here is a merge patch.
	if !reflect.DeepEqual(got.Status, wl.Status) {
		t.Errorf("status changed\ngot:  %+v\nwant: %+v", got.Status, wl.Status)
	}
	for i := range got.Spec.PodSets {
		gotPS, wantPS := &got.Spec.PodSets[i], &wl.Spec.PodSets[i]
		want := corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers:  resourcesOnly(wantPS.Template.Spec.Containers),
			Tolerations: wantPS.Template.Spec.Tolerations,
		}}
		if !reflect.DeepEqual(gotPS.Template, want) {
			t.Errorf("podset %s template = %+v, want container resources and tolerations alone", gotPS.Name, gotPS.Template)
		}
		if gotPS.Count != wantPS.Count || !reflect.DeepEqual(gotPS.TopologyRequest, wantPS.TopologyRequest) {
			t.Errorf("podset %s lost its count or its topology request", gotPS.Name)
		}
	}
}

// TestTransformsAreIdempotent guards the container strips: both write into
// the slice they read (containers[:0]), and an informer re-runs the
// transform on every delivery of an object.
func TestTransformsAreIdempotent(t *testing.T) {
	cfg := testCfg()

	pod := transformPod(t, cfg, fatVictimPod())
	if again := transformPod(t, cfg, pod); !reflect.DeepEqual(again, pod) {
		t.Errorf("second pod transform changed the object\ngot:  %+v\nwant: %+v", again, pod)
	}
	node := transformNode(t, fatDrainedNode())
	if again := transformNode(t, node); !reflect.DeepEqual(again, node) {
		t.Errorf("second node transform changed the object\ngot:  %+v\nwant: %+v", again, node)
	}
	wl := transformWorkload(t, fatWorkload())
	if again := transformWorkload(t, wl); !reflect.DeepEqual(again, wl) {
		t.Errorf("second workload transform changed the object\ngot:  %+v\nwant: %+v", again, wl)
	}
}

// TestTransformsPassForeignObjectsThrough covers the tombstone a deletion
// with a missed watch event delivers: not the typed object the transform
// expects, and it must come back untouched rather than panic.
func TestTransformsPassForeignObjectsThrough(t *testing.T) {
	tombstone := toolscache.DeletedFinalStateUnknown{Key: "tenant-a/victim-a", Obj: fatVictimPod()}
	transforms := map[string]toolscache.TransformFunc{
		"pod":      TransformPod(testCfg()),
		"node":     TransformNode,
		"workload": TransformWorkload,
	}
	for name, transform := range transforms {
		t.Run(name, func(t *testing.T) {
			got, err := transform(tombstone)
			if err != nil {
				t.Fatalf("transform returned %v", err)
			}
			if !reflect.DeepEqual(got, tombstone) {
				t.Errorf("tombstone changed: %+v", got)
			}
		})
	}
}

// blockSnapshot is the only reader of Pod fields: the reconcilers' own pod
// reads (spec.nodeName, status.phase, the kueue workload annotation) go
// through the same fields Build does.
func blockSnapshot(cfg *config.Config, nodes []corev1.Node, pods []corev1.Pod) *snapshot.Snapshot {
	return snapshot.Build(snapshot.Input{
		Level:      levelBlock,
		GroupLevel: levelRack,
		Nodes:      nodes,
		Pods:       pods,
		Self:       heroKey,
		Cfg:        cfg,
	})
}

// nodeReads is everything the controller derives from Node objects.
type nodeReads struct {
	// Two levels, because the level is a node label key: a snapshot keyed
	// by block and one keyed by rack read different labels.
	Blocks *snapshot.Snapshot
	Racks  *snapshot.Snapshot
	// Owners maps node name to the hero its drain taint belongs to.
	Owners map[string]string
	Drains []drainView
}

type drainView struct {
	Owner     string
	Nodes     []string
	StartedAt time.Time
}

func readNodes(cfg *config.Config, nodes []corev1.Node, pods []corev1.Pod) nodeReads {
	out := nodeReads{
		Blocks: blockSnapshot(cfg, nodes, pods),
		Racks: snapshot.Build(snapshot.Input{
			Level: levelRack, Nodes: nodes, Pods: pods, Self: heroKey, Cfg: cfg,
		}),
		Owners: map[string]string{},
	}
	for i := range nodes {
		if owner, ok := taint.Owner(&nodes[i], cfg.TaintKey); ok {
			out.Owners[nodes[i].Name] = owner.String()
		}
	}
	for owner, drain := range taint.FindDrains(nodes, cfg.TaintKey) {
		out.Drains = append(out.Drains, drainView{
			Owner: owner.String(), Nodes: drain.Nodes, StartedAt: drain.StartedAt,
		})
	}
	slices.SortFunc(out.Drains, func(a, b drainView) int { return cmp.Compare(a.Owner, b.Owner) })
	return out
}

// workloadReads is everything the controller derives from Workload
// objects.
type workloadReads struct {
	IsHero         bool
	NotHeroReason  hero.NotHeroReason
	Demand         []hero.Chunk
	GPURequest     resource.Quantity
	PodCount       int32
	Priority       int32
	RequiredLevels []string
	GroupingLevel  string
	GroupingOK     bool
	// Stuck per detection mode; the three read the condition differently.
	Stuck   map[string]bool
	Victims []selection.VictimWorkload
	// HeroDomains are the domains the workload's topology assignment
	// marks as hero-occupied.
	HeroDomains []string
}

func readWorkload(cfg *config.Config, wl *kueue.Workload, nodes []corev1.Node) workloadReads {
	isHero, reason := hero.IsHero(wl, heroClusterQueue(cfg), cfg)
	groupingLevel, groupingOK := hero.GroupingLevel(wl, topologyLevels, levelBlock)
	out := workloadReads{
		IsHero:         isHero,
		NotHeroReason:  reason,
		Demand:         hero.Demand(wl, cfg.GPUResourceName),
		GPURequest:     hero.GPURequest(wl, cfg.GPUResourceName),
		PodCount:       hero.PodCount(wl),
		Priority:       hero.Priority(wl),
		RequiredLevels: hero.RequiredTopologyLevels(wl),
		GroupingLevel:  groupingLevel,
		GroupingOK:     groupingOK,
		Stuck: map[string]bool{
			string(config.DetectionAuto):    hero.IsStuckTASNoFit(wl, config.DetectionAuto),
			string(config.DetectionMessage): hero.IsStuckTASNoFit(wl, config.DetectionMessage),
			string(config.DetectionReason):  hero.IsStuckTASNoFit(wl, config.DetectionReason),
		},
		Victims: selection.GroupVictims(
			&snapshot.Domain{Victims: []snapshot.Victim{{Workload: heroKey}}},
			map[types.NamespacedName]*kueue.Workload{heroKey: wl},
			nil,
			now,
		),
	}
	s := snapshot.Build(snapshot.Input{
		Level: levelBlock, Nodes: nodes, OtherHeroes: []kueue.Workload{*wl}, Cfg: cfg,
	})
	for id, d := range s.Domains {
		if d.HasOtherHero {
			out.HeroDomains = append(out.HeroDomains, id)
		}
	}
	slices.Sort(out.HeroDomains)
	return out
}

// fatVictimPod is a Kueue-managed GPU pod carrying everything an API
// server puts on a pod. Only the node name, the phase, the labels, the
// kueue workload annotation and the GPU containers are read anywhere.
func fatVictimPod() *corev1.Pod {
	pod := testingpod.MakePod("victim-a", "tenant-a").
		NodeName("n1").
		Request(gpu, "4").
		Limit(gpu, "4").
		Request(corev1.ResourceCPU, "16").
		Annotation(kueue.WorkloadAnnotation, "wl-a").
		Label("app", appTrainer).
		StatusPhase(corev1.PodRunning).
		Obj()
	pod.Spec.Containers = append(pod.Spec.Containers,
		// A second GPU container: podGPURequest sums across containers.
		corev1.Container{
			Name:  "trainer-aux",
			Image: imageTrainer,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{gpu: resource.MustParse("2")},
			},
		},
		// CPU-only sidecar: contributes nothing to the GPU sum.
		corev1.Container{
			Name:  "log-shipper",
			Image: "fluentd:v1",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
			},
		},
	)
	return bloatPod(pod)
}

// fatPods is one pod per branch of the pod readers: victim, limits-only
// non-kueue, non-blocking-labeled, non-kueue (non-reclaimable),
// terminating, terminal, CPU-only, unscheduled, and a victim in the other
// block.
func fatPods() []corev1.Pod {
	// The victim's second pod, on another node of the same block.
	sibling := bloatPod(testingpod.MakePod("victim-a2", "tenant-a").
		NodeName("n8").Request(gpu, "6").
		Annotation(kueue.WorkloadAnnotation, "wl-a").
		Label("app", appTrainer).
		StatusPhase(corev1.PodRunning).Obj())
	limitsOnly := bloatPod(testingpod.MakePod("infra-agent", "infra").
		NodeName("n1").Limit(gpu, "1").StatusPhase(corev1.PodRunning).Obj())
	nonBlocking := bloatPod(testingpod.MakePod("hpc-check", "hpc").
		NodeName("n1").Request(gpu, "2").Label("app", "hpc-verification").
		StatusPhase(corev1.PodRunning).Obj())
	daemon := bloatPod(testingpod.MakePod("gpu-exporter", "kube-system").
		NodeName("n2").Request(gpu, "1").StatusPhase(corev1.PodRunning).Obj())
	terminating := bloatPod(testingpod.MakePod("dying", "tenant-a").
		NodeName("n2").Request(gpu, "8").
		Annotation(kueue.WorkloadAnnotation, "wl-dying").
		StatusPhase(corev1.PodRunning).
		DeletionTimestamp(now.Add(-time.Minute)).Obj())
	finished := bloatPod(testingpod.MakePod("done", "tenant-a").
		NodeName("n2").Request(gpu, "8").
		Annotation(kueue.WorkloadAnnotation, "wl-done").
		StatusPhase(corev1.PodSucceeded).Obj())
	cpuOnly := bloatPod(testingpod.MakePod("cpu-sidecar", "tenant-a").
		NodeName("n1").Request(corev1.ResourceCPU, "4").
		Annotation(kueue.WorkloadAnnotation, "wl-cpu").
		StatusPhase(corev1.PodRunning).Obj())
	unscheduled := bloatPod(testingpod.MakePod("queued", "tenant-a").
		Request(gpu, "8").
		Annotation(kueue.WorkloadAnnotation, "wl-queued").
		StatusPhase(corev1.PodPending).Obj())
	otherBlock := bloatPod(testingpod.MakePod("victim-b", "tenant-b").
		NodeName("n4").Request(gpu, "8").
		Annotation(kueue.WorkloadAnnotation, "wl-b").
		StatusPhase(corev1.PodRunning).Obj())

	return []corev1.Pod{
		*fatVictimPod(), *sibling, *limitsOnly, *nonBlocking, *daemon,
		*terminating, *finished, *cpuOnly, *unscheduled, *otherBlock,
	}
}

// bloatPod adds the parts of a pod object no reader touches.
func bloatPod(pod *corev1.Pod) *corev1.Pod {
	pod.UID = types.UID(pod.Name + "-uid")
	pod.ResourceVersion = "424242"
	pod.ManagedFields = fatManagedFields()
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "batch/v1", Kind: "Job", Name: pod.Name + "-job", UID: "job-uid",
	}}
	pod.Finalizers = []string{"kueue.x-k8s.io/managed"}
	pod.Annotations["kubectl.kubernetes.io/last-applied-configuration"] = `{"apiVersion":"v1","kind":"Pod"}`
	// A GPU-requesting init container: podGPURequest sums over containers
	// only, so dropping this must not move any number.
	pod.Spec.InitContainers = []corev1.Container{{
		Name:  "warm-cache",
		Image: imageInit,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{gpu: resource.MustParse("8")},
		},
	}}
	pod.Spec.Volumes = []corev1.Volume{{
		Name:         volumeName,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}}
	pod.Spec.ServiceAccountName = appTrainer
	pod.Spec.SchedulerName = "kueue-scheduler"
	pod.Spec.NodeSelector = map[string]string{"nvidia.com/gpu.present": "true"}
	pod.Spec.Tolerations = []corev1.Toleration{{Key: "example.com/spot", Operator: corev1.TolerationOpExists}}
	pod.Status = corev1.PodStatus{
		Phase:  pod.Status.Phase,
		HostIP: "10.0.0.1",
		PodIP:  "10.1.2.3",
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		},
		ContainerStatuses: []corev1.ContainerStatus{{
			Name: "c", Image: imageTrainer, RestartCount: 3, Ready: true,
		}},
		StartTime: &metav1.Time{Time: now.Add(-3 * time.Hour)},
	}
	return pod
}

// fatDrainedNode is a node carrying this controller's drain taint for
// heroKey, plus everything an API server puts on a node.
func fatDrainedNode() *corev1.Node {
	return bloatNode(gpuNode("n2", "b1", "r1").
		Annotation(taint.OwnerAnnotation, heroKey.String()).
		Annotation(taint.StartedAtAnnotation, now.Add(-10*time.Minute).UTC().Format(time.RFC3339)).
		Annotation(taint.NudgeAnnotation, now.Add(-2*time.Minute).UTC().Format(time.RFC3339)).
		Taints(drainTaint).
		Obj())
}

// fatNodes is one node per branch of the node readers: plain, drained for
// heroKey, drained for another hero, our taint key with no owner
// annotation, not ready, cordoned, foreign NoSchedule taint, a
// PreferNoSchedule taint that disqualifies nothing, and one outside the
// topology.
func fatNodes() []corev1.Node {
	plain := bloatNode(gpuNode("n1", "b1", "r1").Obj())
	foreignDrain := bloatNode(gpuNode("n3", "b1", "r2").
		Annotation(taint.OwnerAnnotation, otherKey.String()).
		Annotation(taint.StartedAtAnnotation, now.Add(-20*time.Minute).UTC().Format(time.RFC3339)).
		Taints(drainTaint).Obj())
	unattributed := bloatNode(gpuNode("n4", "b2", "r3").Taints(drainTaint).Obj())
	notReady := bloatNode(gpuNode("n5", "b2", "r3").NotReady().Obj())
	cordoned := bloatNode(gpuNode("n6", "b2", "r4").Unschedulable().Obj())
	foreignTaint := bloatNode(gpuNode("n7", "b2", "r4").Taints(corev1.Taint{
		Key: "example.com/maintenance", Effect: corev1.TaintEffectNoSchedule,
	}).Obj())
	softTaint := bloatNode(gpuNode("n8", "b1", "r1").Taints(corev1.Taint{
		Key: "example.com/spot", Effect: corev1.TaintEffectPreferNoSchedule,
	}).Obj())
	unlabeled := bloatNode(testingnode.MakeNode("unlabeled").
		StatusAllocatable(corev1.ResourceList{gpu: resource.MustParse("8")}).Ready().Obj())

	return []corev1.Node{
		*plain, *fatDrainedNode(), *foreignDrain, *unattributed, *notReady,
		*cordoned, *foreignTaint, *softTaint, *unlabeled,
	}
}

// gpuNode is a Ready 8-GPU node in the given block and rack.
func gpuNode(name, block, rack string) *testingnode.NodeWrapper {
	return testingnode.MakeNode(name).
		Label(levelBlock, block).
		Label(levelRack, rack).
		Label(corev1.LabelHostname, name).
		StatusAllocatable(corev1.ResourceList{
			gpu:                resource.MustParse("8"),
			corev1.ResourceCPU: resource.MustParse("128"),
		}).
		Ready()
}

// bloatNode adds the parts of a node object no reader touches. All of them
// are in the status: a node's metadata and spec are read, and written,
// whole.
func bloatNode(node *corev1.Node) *corev1.Node {
	node.UID = types.UID(node.Name + "-uid")
	node.ResourceVersion = "99"
	node.ManagedFields = fatManagedFields()
	node.Spec.ProviderID = "coreweave://" + node.Name
	node.Spec.PodCIDR = "10.244.0.0/24"
	node.Status.Capacity = corev1.ResourceList{
		gpu:                resource.MustParse("8"),
		corev1.ResourceCPU: resource.MustParse("128"),
	}
	node.Status.Images = []corev1.ContainerImage{
		{Names: []string{imageTrainer}, SizeBytes: 12 << 30},
		{Names: []string{imageInit}, SizeBytes: 4 << 20},
	}
	node.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.2"}}
	node.Status.NodeInfo = corev1.NodeSystemInfo{KubeletVersion: "v1.34.1", OSImage: "Ubuntu 24.04"}
	node.Status.DaemonEndpoints = corev1.NodeDaemonEndpoints{
		KubeletEndpoint: corev1.DaemonEndpoint{Port: 10250},
	}
	node.Status.Conditions = append(node.Status.Conditions,
		corev1.NodeCondition{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse},
		corev1.NodeCondition{Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse},
	)
	return node
}

// fatWorkload is a hero Workload with two podsets — one carrying the slice
// pair, one without — a full admission with a topology assignment, and the
// pod templates kueue hangs off every podset.
//
// Its conditions deliberately contradict each other (pending on a TAS
// no-fit AND admitted): one fixture has to make every reader return a
// non-trivial value, and whether the object is semantically coherent is
// nothing a transform can care about.
func fatWorkload() *kueue.Workload {
	cfg := testCfg()
	workers := utiltesting.MakePodSet("workers", 16).
		SliceRequiredTopologyRequest(levelBlock).
		SliceSizeTopologyRequest(8).
		RequiredTopologyRequest(levelBlock).
		Request(gpu, "8").
		Request(corev1.ResourceCPU, "32").
		Toleration(corev1.Toleration{
			Key:      cfg.TaintKey,
			Operator: corev1.TolerationOpEqual,
			Value:    heroCQName,
			Effect:   corev1.TaintEffectNoSchedule,
		}).
		Obj()
	// No slice pair: contributes neither drain demand nor a drain level.
	aux := utiltesting.MakePodSet("aux", 4).
		RequiredTopologyRequest(levelRack).
		Limit(gpu, "1").
		Obj()
	bloatTemplate(&workers.Template)
	bloatTemplate(&aux.Template)

	admission := utiltesting.MakeAdmission(heroCQName).PodSets(
		utiltesting.MakePodSetAssignment("workers").
			Count(16).
			Assignment(gpu, "gpu-flavor", "128").
			TopologyAssignment(utiltesting.MakeTopologyAssignment([]string{levelBlock}).
				Domain(utiltas.TopologyDomainAssignment{Values: []string{"b1"}, Count: 16}).
				Obj()).
			Obj(),
		utiltesting.MakePodSetAssignment("aux").Count(4).Obj(),
	).Obj()

	wl := utiltesting.MakeWorkload(heroKey.Name, heroKey.Namespace).
		UID("hero-uid").
		Creation(now.Add(-4*time.Hour)).
		Queue("hero-lq").
		Label("kueue.x-k8s.io/job-uid", "job-uid").
		Annotation("hero.coreweave.com/deactivated-for", otherKey.String()).
		WorkloadPriorityClassRef(cfg.HeroPriorityClassName).
		Priority(1000).
		PodSets(*workers, *aux).
		Admission(admission).
		Conditions(
			metav1.Condition{
				Type:               kueue.WorkloadQuotaReserved,
				Status:             metav1.ConditionFalse,
				Reason:             "Pending",
				Message:            msgTASNoFit,
				LastTransitionTime: metav1.NewTime(now.Add(-30 * time.Minute)),
			},
			metav1.Condition{
				Type:               kueue.WorkloadAdmitted,
				Status:             metav1.ConditionTrue,
				Reason:             "ByTest",
				LastTransitionTime: metav1.NewTime(now.Add(-90 * time.Minute)),
			},
		).
		Obj()
	wl.ManagedFields = fatManagedFields()
	wl.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "batch/v1", Kind: "Job", Name: "hero-job", UID: "job-uid",
	}}
	wl.Finalizers = []string{"kueue.x-k8s.io/resource-in-use"}
	return wl
}

// bloatTemplate fills in the pod template kueue copies from the job. Only
// container resources and tolerations are read from one.
func bloatTemplate(tmpl *corev1.PodTemplateSpec) {
	tmpl.ObjectMeta = metav1.ObjectMeta{
		Labels: map[string]string{"app": appTrainer},
		Annotations: map[string]string{
			"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"v1"}`,
		},
	}
	tmpl.Spec.Volumes = []corev1.Volume{{
		Name:         volumeName,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}}
	tmpl.Spec.InitContainers = []corev1.Container{{
		Name:  "warm-cache",
		Image: imageInit,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{gpu: resource.MustParse("8")},
		},
	}}
	tmpl.Spec.NodeSelector = map[string]string{"nvidia.com/gpu.present": "true"}
	tmpl.Spec.ServiceAccountName = appTrainer
	for i := range tmpl.Spec.Containers {
		c := &tmpl.Spec.Containers[i]
		c.Image = imageTrainer
		c.Command = []string{"/usr/bin/train"}
		c.Env = []corev1.EnvVar{{Name: "NCCL_DEBUG", Value: "INFO"}}
		c.VolumeMounts = []corev1.VolumeMount{{Name: volumeName, MountPath: "/scratch"}}
	}
}

// heroClusterQueue completes hero identification. ClusterQueues are cached
// whole, so this one needs no fattening.
func heroClusterQueue(cfg *config.Config) *kueue.ClusterQueue {
	return utiltesting.MakeClusterQueue(heroCQName).Label(cfg.HeroCQLabelKey, "true").Obj()
}

func fatManagedFields() []metav1.ManagedFieldsEntry {
	return []metav1.ManagedFieldsEntry{{
		Manager:    "kube-controller-manager",
		Operation:  metav1.ManagedFieldsOperationUpdate,
		APIVersion: "v1",
		Time:       &metav1.Time{Time: now.Add(-time.Hour)},
		FieldsType: "FieldsV1",
		FieldsV1:   &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:containers":{}}}`)},
	}}
}

// The transform helpers run on a deep copy, the way an informer runs them:
// in place, on the object as it arrives. The copy keeps the fixture fat for
// the comparison.

func transformPod(t *testing.T, cfg *config.Config, pod *corev1.Pod) *corev1.Pod {
	t.Helper()
	obj, err := TransformPod(cfg)(pod.DeepCopy())
	if err != nil {
		t.Fatalf("TransformPod: %v", err)
	}
	out, ok := obj.(*corev1.Pod)
	if !ok {
		t.Fatalf("TransformPod returned %T, want *corev1.Pod", obj)
	}
	return out
}

func transformPods(t *testing.T, cfg *config.Config, pods []corev1.Pod) []corev1.Pod {
	t.Helper()
	out := make([]corev1.Pod, 0, len(pods))
	for i := range pods {
		out = append(out, *transformPod(t, cfg, &pods[i]))
	}
	return out
}

func transformNode(t *testing.T, node *corev1.Node) *corev1.Node {
	t.Helper()
	obj, err := TransformNode(node.DeepCopy())
	if err != nil {
		t.Fatalf("TransformNode: %v", err)
	}
	out, ok := obj.(*corev1.Node)
	if !ok {
		t.Fatalf("TransformNode returned %T, want *corev1.Node", obj)
	}
	return out
}

func transformNodes(t *testing.T, nodes []corev1.Node) []corev1.Node {
	t.Helper()
	out := make([]corev1.Node, 0, len(nodes))
	for i := range nodes {
		out = append(out, *transformNode(t, &nodes[i]))
	}
	return out
}

func transformWorkload(t *testing.T, wl *kueue.Workload) *kueue.Workload {
	t.Helper()
	obj, err := TransformWorkload(wl.DeepCopy())
	if err != nil {
		t.Fatalf("TransformWorkload: %v", err)
	}
	out, ok := obj.(*kueue.Workload)
	if !ok {
		t.Fatalf("TransformWorkload returned %T, want *kueue.Workload", obj)
	}
	return out
}

// resourcesOnly is what a podset template's containers are expected to
// keep: their resource requirements, every container of them.
func resourcesOnly(containers []corev1.Container) []corev1.Container {
	out := make([]corev1.Container, 0, len(containers))
	for i := range containers {
		out = append(out, corev1.Container{Resources: containers[i].Resources})
	}
	return out
}

// render is the comparison form for reader results: JSON rather than the
// values, because resource.Quantity carries a cached string that two equal
// quantities need not share, and because every field of a result type
// shows up in the rendering — including fields added to it later.
func render(t *testing.T, v any) string {
	t.Helper()
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("marshalling %T: %v", v, err)
	}
	return string(out)
}
