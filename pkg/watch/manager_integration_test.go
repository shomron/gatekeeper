package watch_test

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"sigs.k8s.io/controller-runtime/pkg/event"

	"golang.org/x/sync/errgroup"

	"github.com/onsi/gomega"
	"github.com/open-policy-agent/gatekeeper/pkg/watch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

func setupManager(t *testing.T) (manager.Manager, *watch.Manager) {
	t.Helper()

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	ctrl.SetLogger(zap.Logger(true))
	mgr, err := manager.New(cfg, manager.Options{MetricsBindAddress: "0"})
	if err != nil {
		t.Fatalf("setting up controller manager: %s", err)
	}
	wm, err := watch.New(mgr.GetCache())
	if err != nil {
		t.Fatalf("could not create watch manager: %s", err)
	}
	if err := mgr.Add(wm); err != nil {
		t.Fatalf("could not add watch manager to manager: %s", err)
	}
	return mgr, wm
}

// Verify that an unknown resource will return an error
func TestRegistrar_AddUnknown(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	mgr, wm := setupManager(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grp, ctx := errgroup.WithContext(ctx)

	grp.Go(func() error {
		return mgr.Start(ctx.Done())
	})

	events := make(chan event.GenericEvent)
	r, err := wm.NewRegistrar("test", events)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "creating registrar")

	err = r.AddWatch(schema.GroupVersionKind{
		Group:   "i",
		Version: "donot",
		Kind:    "exist",
	})
	g.Expect(err).To(gomega.HaveOccurred(), "AddWatch should have failed due to unknown GVK")

	cancel()
	_ = grp.Wait()
}

func TestRegistrar_Add_Events() {

}
