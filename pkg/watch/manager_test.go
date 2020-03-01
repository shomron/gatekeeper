// +build disabled

package watch

import (
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

func newForTest() (*Manager, error) {
	metrics, err := newStatsReporter()
	if err != nil {
		return nil, err
	}
	wm := &Manager{
		stopper:      func() {},
		managedKinds: newRecordKeeper(),
		watchedKinds: make(map[schema.GroupVersionKind]vitals),
		metrics:      metrics,
	}
	wm.managedKinds.mgr = wm
	wm.started.Store(false)
	return wm, nil
}

func newFakeMgr(wm *Manager) (manager.Manager, error) {
	return &fakeMgr{}, nil
}

var _ manager.Manager = &fakeMgr{}

type fakeMgr struct{}

func (m *fakeMgr) Add(runnable manager.Runnable) error {
	return nil
}

func (m *fakeMgr) SetFields(interface{}) error {
	return nil
}

func (m *fakeMgr) Start(c <-chan struct{}) error {
	<-c
	return nil
}

func (m *fakeMgr) GetConfig() *rest.Config {
	return nil
}

func (m *fakeMgr) GetScheme() *runtime.Scheme {
	return nil
}

func (m *fakeMgr) GetClient() client.Client {
	return nil
}

func (m *fakeMgr) GetFieldIndexer() client.FieldIndexer {
	return nil
}

func (m *fakeMgr) GetCache() cache.Cache {
	return nil
}

func (m *fakeMgr) GetEventRecorderFor(name string) record.EventRecorder {
	return nil
}

func (m *fakeMgr) GetRESTMapper() meta.RESTMapper {
	return nil
}

func (m *fakeMgr) GetAPIReader() client.Reader {
	return nil
}

func (m *fakeMgr) GetWebhookServer() *webhook.Server {
	return nil
}

func (m *fakeMgr) AddHealthzCheck(string, healthz.Checker) error {
	return nil
}

func (m *fakeMgr) AddReadyzCheck(string, healthz.Checker) error {
	return nil
}

var _ Discovery = &fakeClient{}

func newDiscoveryFactory(notFound bool, kinds ...string) func(*rest.Config) (Discovery, error) {
	return func(*rest.Config) (Discovery, error) {
		return &fakeClient{notFound: notFound, kinds: kinds}, nil
	}
}

type fakeClient struct {
	notFound bool
	kinds    []string
}

func (c *fakeClient) ServerResourcesForGroupVersion(s string) (*metav1.APIResourceList, error) {
	if c.notFound {
		return &metav1.APIResourceList{GroupVersion: s}, nil
	}
	rsrs := make([]metav1.APIResource, len(c.kinds))
	for _, k := range c.kinds {
		rsrs = append(rsrs, metav1.APIResource{Kind: k})
	}
	return &metav1.APIResourceList{GroupVersion: s, APIResources: rsrs}, nil
}

func newChange(kind string, r ...*Registrar) map[schema.GroupVersionKind]vitals {
	rs := make(map[*Registrar]bool)
	for _, v := range r {
		rs[v] = true
	}
	gvk := makeGvk(kind)
	return map[schema.GroupVersionKind]vitals{gvk: {gvk: gvk, registrars: rs}}
}

func makeGvk(k string) schema.GroupVersionKind {
	return schema.GroupVersionKind{Kind: k}
}

func waitForWatchManagerStart(wm *Manager) bool {
	for i := 0; i < 10; i++ {
		if wm.started.Load().(bool) == true {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestRegistrar(t *testing.T) {
	wm, err := newForTest()
	if err != nil {
		t.Fatalf("Error creating Manager: %s", err)
	}
	defer wm.close()
	reg, err := wm.NewRegistrar("foo", nil)
	if err != nil {
		t.Fatalf("Error setting up registrar: %s", err)
	}
	if err := reg.AddWatch(makeGvk("FooCRD")); err != nil {
		t.Fatalf("Error adding watch: %s", err)
	}

	t.Run("Single Add Watch", func(t *testing.T) {
		expectedAdded := newChange("FooCRD", reg)
		managedKinds, err := wm.managedKinds.Get()
		if err != nil {
			t.Errorf("Could not get managedKinds: %s", err)
		}
		added, removed, changed, err := wm.gatherChanges(managedKinds)
		if diff := cmp.Diff(added, expectedAdded, cmp.AllowUnexported(vitals{})); diff != "" {
			t.Error(diff)
		}
		if len(removed) != 0 {
			t.Errorf("removed = %s, wanted empty map", spew.Sdump(removed))
		}
		if len(changed) != 0 {
			t.Errorf("changed = %s, wanted empty map", spew.Sdump(changed))
		}
		if err != nil {
			t.Errorf("err = %s, want nil", err)
		}
		b, err := wm.updateManager()
		if err != nil {
			t.Errorf("Could not update manager: %s", err)
		}
		if b == false {
			t.Errorf("Manager not restarted on first add")
		}
		if waitForWatchManagerStart(wm) == false {
			t.Errorf("Watch manager was not set to started")
		}
	})

	t.Run("Second add watch does nothing", func(t *testing.T) {
		if err := reg.AddWatch(makeGvk("FooCRD")); err != nil {
			t.Fatalf("Error adding second watch: %s", err)
		}
		managedKinds, err := wm.managedKinds.Get()
		if err != nil {
			t.Errorf("Could not get managedKinds: %s", err)
		}
		added, removed, changed, err := wm.gatherChanges(managedKinds)
		if len(added) != 0 {
			t.Errorf("added = %s, wanted empty map", spew.Sdump(added))
		}
		if len(removed) != 0 {
			t.Errorf("removed = %s, wanted empty map", spew.Sdump(removed))
		}
		if len(changed) != 0 {
			t.Errorf("changed = %s, wanted empty map", spew.Sdump(changed))
		}
		if err != nil {
			t.Errorf("err = %s, want nil", err)
		}
		b, err := wm.updateManager()
		if err != nil {
			t.Errorf("Could not update manager: %s", err)
		}
		if b == true {
			t.Errorf("Manager restarted, wanted no op")
		}
		if waitForWatchManagerStart(wm) == false {
			t.Errorf("Watch manager was not set to started")
		}
	})

	reg2, err := wm.NewRegistrar("bar", nil)
	if err != nil {
		t.Fatalf("Error setting up 2nd registrar: %s", err)
	}
	t.Run("New registrar makes for a restart", func(t *testing.T) {
		if err := reg2.AddWatch(makeGvk("FooCRD")); err != nil {
			t.Fatalf("Error adding watch: %s", err)
		}
		expectedChanged := newChange("FooCRD", reg, reg2)
		managedKinds, err := wm.managedKinds.Get()
		if err != nil {
			t.Errorf("Could not get managedKinds: %s", err)
		}
		added, removed, changed, err := wm.gatherChanges(managedKinds)
		if len(added) != 0 {
			t.Errorf("added = %s, wanted empty map", spew.Sdump(added))
		}
		if len(removed) != 0 {
			t.Errorf("removed = %s, wanted empty map", spew.Sdump(removed))
		}
		if diff := cmp.Diff(changed, expectedChanged, cmp.AllowUnexported(vitals{})); diff != "" {
			t.Error(diff)
		}
		if err != nil {
			t.Errorf("err = %s, want nil", err)
		}
		b, err := wm.updateManager()
		if err != nil {
			t.Errorf("Could not update manager: %s", err)
		}
		if b == false {
			t.Errorf("Manager not restarted")
		}
		if waitForWatchManagerStart(wm) == false {
			t.Errorf("Watch manager was not set to started")
		}
	})

	t.Run("First remove makes for a change", func(t *testing.T) {
		if err := reg2.RemoveWatch(makeGvk("FooCRD")); err != nil {
			t.Fatalf("Error removing watch: %s", err)
		}
		expectedChanged := newChange("FooCRD", reg)
		managedKinds, err := wm.managedKinds.Get()
		if err != nil {
			t.Errorf("Could not get managedKinds: %s", err)
		}
		added, removed, changed, err := wm.gatherChanges(managedKinds)
		if len(added) != 0 {
			t.Errorf("added = %s, wanted empty map", spew.Sdump(added))
		}
		if len(removed) != 0 {
			t.Errorf("removed = %s, wanted empty map", spew.Sdump(removed))
		}
		if diff := cmp.Diff(changed, expectedChanged, cmp.AllowUnexported(vitals{})); diff != "" {
			t.Error(diff)
		}
		if err != nil {
			t.Errorf("err = %s, want nil", err)
		}
		b, err := wm.updateManager()
		if err != nil {
			t.Errorf("Could not update manager: %s", err)
		}
		if b == false {
			t.Errorf("Manager not restarted")
		}
		if waitForWatchManagerStart(wm) == false {
			t.Errorf("Watch manager was not set to started")
		}
	})

	t.Run("Second remove makes for a remove", func(t *testing.T) {
		if err := reg.RemoveWatch(makeGvk("FooCRD")); err != nil {
			t.Fatalf("Error removing watch: %s", err)
		}
		expectedRemoved := newChange("FooCRD", reg)
		managedKinds, err := wm.managedKinds.Get()
		if err != nil {
			t.Errorf("Could not get managedKinds: %s", err)
		}
		added, removed, changed, err := wm.gatherChanges(managedKinds)
		if len(added) != 0 {
			t.Errorf("added = %s, wanted empty map", spew.Sdump(added))
		}
		if diff := cmp.Diff(removed, expectedRemoved, cmp.AllowUnexported(vitals{})); diff != "" {
			t.Error(diff)
		}
		if len(changed) != 0 {
			t.Errorf("changed = %s, wanted empty map", spew.Sdump(removed))
		}
		if err != nil {
			t.Errorf("err = %s, want nil", err)
		}
		b, err := wm.updateManager()
		if err != nil {
			t.Errorf("Could not update manager: %s", err)
		}
		if b == false {
			t.Errorf("Manager not restarted")
		}
		if waitForWatchManagerStart(wm) == false {
			t.Errorf("Watch manager was not set to started")
		}
	})

	t.Run("Single Add Waits For CRD Available", func(t *testing.T) {
		if err := reg.AddWatch(makeGvk("FooCRD")); err != nil {
			t.Fatalf("Error adding watch: %s", err)
		}
		wm.newDiscovery = newDiscoveryFactory(true)
		expectedAdded := newChange("FooCRD", reg)
		managedKinds, err := wm.managedKinds.Get()
		if err != nil {
			t.Errorf("Could not get managedKinds: %s", err)
		}
		added, removed, changed, err := wm.gatherChanges(managedKinds)
		if diff := cmp.Diff(added, expectedAdded, cmp.AllowUnexported(vitals{})); diff != "" {
			t.Error(diff)
		}
		if len(removed) != 0 {
			t.Errorf("removed = %s, wanted empty map", spew.Sdump(removed))
		}
		if len(changed) != 0 {
			t.Errorf("changed = %s, wanted empty map", spew.Sdump(changed))
		}
		if err != nil {
			t.Errorf("err = %s, want nil", err)
		}
		b, err := wm.updateManager()
		if err != nil {
			t.Errorf("Could not update manager: %s", err)
		}
		if b == true {
			t.Errorf("Manager should not have restarted while CRD is pending")
		}

		wm.newDiscovery = newDiscoveryFactory(false, "FooCRD")
		b, err = wm.updateManager()
		if err != nil {
			t.Errorf("Could not update manager: %s", err)
		}
		if b == false {
			t.Errorf("Manager should have updated now that CRD is found")
		}
		if waitForWatchManagerStart(wm) == false {
			t.Errorf("Watch manager was not set to started")
		}
	})

	t.Run("Replace works", func(t *testing.T) {
		if err := reg.ReplaceWatch([]schema.GroupVersionKind{}); err != nil {
			t.Fatalf("Error replacing watch: %s", err)
		}
		expectedRemoved := newChange("FooCRD", reg)
		managedKinds, err := wm.managedKinds.Get()
		if err != nil {
			t.Errorf("Could not get managedKinds: %s", err)
		}
		added, removed, changed, err := wm.gatherChanges(managedKinds)
		if len(added) != 0 {
			t.Errorf("added = %s, wanted empty map", spew.Sdump(removed))
		}
		if diff := cmp.Diff(removed, expectedRemoved, cmp.AllowUnexported(vitals{})); diff != "" {
			t.Error(diff)
		}
		if len(changed) != 0 {
			t.Errorf("changed = %s, wanted empty map", spew.Sdump(changed))
		}
		if err != nil {
			t.Errorf("err = %s, want nil", err)
		}
		b, err := wm.updateManager()
		if err != nil {
			t.Errorf("Could not update manager: %s", err)
		}
		if b == false {
			t.Errorf("Manager should have updated now that CRD is found")
		}
	})

	t.Run("Missing existing resources dumped on restart", func(t *testing.T) {
		if err := reg.ReplaceWatch([]schema.GroupVersionKind{makeGvk("initialCRD"), makeGvk("secondCRD")}); err != nil {
			t.Fatalf("Error replacing watch: %s", err)
		}
		wm.newDiscovery = newDiscoveryFactory(false, "initialCRD", "secondCRD")
		b, err := wm.updateManager()
		if err != nil {
			t.Errorf("Could not update manager: %s", err)
		}
		if b == false {
			t.Errorf("Manager should have updated for new CRDs")
		}
		if err := reg.ReplaceWatch([]schema.GroupVersionKind{makeGvk("initialCRD")}); err != nil {
			t.Fatalf("Error replacing watch: %s", err)
		}
		wm.newDiscovery = newDiscoveryFactory(true, "initialCRD")
		b, err = wm.updateManager()
		if err != nil {
			t.Errorf("Could not update manager: %s", err)
		}
		if b == false {
			t.Errorf("Manager should have updated for removed CRD")
		}
		if len(wm.watchedKinds) != 0 {
			t.Errorf("Manager should be watching zero kinds, watching: %v", wm.watchedKinds)
		}
	})

	if waitForWatchManagerStart(wm) == false {
		t.Errorf("Watch manager was not set to started")
	}

	t.Run("Manager restarts when not started", func(t *testing.T) {
		wm.started.Store(false)
		b, err := wm.updateManager()
		if err != nil {
			t.Errorf("Could not update manager: %s", err)
		}
		if b == false {
			t.Errorf("Manager not restarted")
		}
	})
}
