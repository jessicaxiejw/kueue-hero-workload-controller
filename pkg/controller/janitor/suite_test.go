// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package janitorctrl

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/index"
)

var (
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client
	scheme    = runtime.NewScheme()

	ctx    context.Context
	cancel context.CancelFunc

	testCfg config.Config
	testNow = time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)

	// nowOffset lets specs jump the reconciler's clock forward.
	nowOffset atomic.Int64
)

func currentNow() time.Time {
	return testNow.Add(time.Duration(nowOffset.Load()))
}

func TestJanitor(t *testing.T) {
	RegisterFailHandler(Fail)
	SetDefaultEventuallyTimeout(15 * time.Second)
	SetDefaultEventuallyPollingInterval(100 * time.Millisecond)
	RunSpecs(t, "Janitor Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))
	ctx, cancel = context.WithCancel(context.TODO())

	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(kueuev1beta2.AddToScheme(scheme))

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "test", "crds")},
		ErrorIfCRDPathMissing: true,
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred())

	testCfg = config.Default()

	// Indexes as in cmd/main.go: the specs must exercise the same field
	// indexes the controller lists through.
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         scheme,
		Metrics:        metricsserver.Options{BindAddress: "0"},
		LeaderElection: false,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(index.Register(ctx, mgr.GetFieldIndexer(), &testCfg)).To(Succeed())

	reconciler := &Reconciler{
		Client:   mgr.GetClient(),
		Recorder: mgr.GetEventRecorderFor("hero-janitor"),
		Cfg:      &testCfg,
		Now:      currentNow,
	}
	Expect(reconciler.SetupWithManager(mgr)).To(Succeed())

	go func() {
		defer GinkgoRecover()
		Expect(mgr.Start(ctx)).To(Succeed())
	}()
	Eventually(mgr.GetCache().WaitForCacheSync).WithArguments(ctx).Should(BeTrue())
})

var _ = AfterSuite(func() {
	cancel()
	Expect(testEnv.Stop()).To(Succeed())
})
