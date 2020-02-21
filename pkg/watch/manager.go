package watch

import (
	"fmt"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/controller-runtime/pkg/event"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var log = logf.Log.WithName("watchManager")

// WatchManager allows us to dynamically configure what kinds are watched
type Manager struct {
	mgr        manager.Manager
	startedMux sync.RWMutex
	stopper    func()
	stopped    chan struct{}
	// started is a bool (which is not thread-safe by default)
	started atomic.Value
	paused  bool
	// managedKinds stores the kinds that should be managed, mapping CRD Kind to CRD Name
	managedKinds *recordKeeper
	// watchedKinds are the kinds that have a currently running constraint controller
	watchedMux   sync.Mutex
	watchedKinds vitalsByGVK
	metrics      *reporter
}

type AddFunction func(manager.Manager) error

func New(mgr manager.Manager, cfg *rest.Config) (*Manager, error) {
	metrics, err := newStatsReporter()
	if err != nil {
		return nil, err
	}
	wm := &Manager{
		mgr:          mgr,
		stopper:      func() {},
		managedKinds: newRecordKeeper(),
		watchedKinds: make(map[schema.GroupVersionKind]vitals),
		metrics:      metrics,
	}
	wm.started.Store(false)
	wm.managedKinds.mgr = wm
	return wm, nil
}

func (wm *Manager) NewRegistrar(parent string, events chan<- event.GenericEvent) (*Registrar, error) {
	return wm.managedKinds.NewRegistrar(parent, events)
}

// Start looks for changes to the watch roster every 5 seconds. This method has a
// benefit compared to restarting the manager every time a controller changes the watch
// of placing an upper bound on how often the manager restarts.
func (wm *Manager) Start(done <-chan struct{}) error {
	return nil // TODO(OREN)
}

func (wm *Manager) close() {
	log.Info("attempting to stop watch manager...")
	wm.startedMux.Lock()
	defer wm.startedMux.Unlock()
	wm.stopper()
	log.Info("waiting for watch manager to shut down")
	if wm.stopped != nil {
		<-wm.stopped
	}
	log.Info("watch manager finished shutting down")
}

func (wm *Manager) GetManagedGVK() []schema.GroupVersionKind {
	return wm.managedKinds.GetGVK()
}

func (wm *Manager) addWatch(gvk schema.GroupVersionKind) error {
	wm.watchedMux.Lock()
	defer wm.watchedMux.Unlock()
	return wm.doAddWatch(gvk)
}

func (wm *Manager) doAddWatch(gvk schema.GroupVersionKind) error {
	// lock acquired by caller

	if _, ok := wm.watchedKinds[gvk]; ok {
		// Already watching.
		return nil
	}

	// Prep for marking as managed below
	// TODO(OREN): Simplify
	m := wm.managedKinds.Get() // Not a deadlock but beware if assumptions change...
	if _, ok := m[gvk]; !ok {
		return fmt.Errorf("could not mark %+v as managed", gvk)
	}

	// TODO(OREN) - will this map correctly to unstructured/structured informers?
	informer, err := wm.mgr.GetCache().GetInformerForKind(gvk)
	if err != nil {
		// This is expected to fail if a CRD is unregistered.
		return fmt.Errorf("getting informer for kind: %+v %w", gvk, err)
	}
	informer.AddEventHandler(wm)

	// Mark it as watched TODO(OREN) simplify
	wm.watchedKinds[gvk] = m[gvk]
	return nil
}

type removableCache interface {
	Remove(obj runtime.Object) error
}

func (wm *Manager) removeWatch(gvk schema.GroupVersionKind) error {
	wm.watchedMux.Lock()
	defer wm.watchedMux.Unlock()
	return wm.doRemoveWatch(gvk)
}

func (wm *Manager) doRemoveWatch(gvk schema.GroupVersionKind) error {
	// lock acquired by caller

	if _, ok := wm.watchedKinds[gvk]; !ok {
		// Not watching.
		return fmt.Errorf("not watching: %+v", gvk)
	}

	// Skip if there are additional watchers that would prevent us from removing it
	m := wm.managedKinds.Get() // Not a deadlock but beware if assumptions change...
	if _, ok := m[gvk]; ok {
		return nil
	}

	c := wm.mgr.GetCache()
	rc, ok := c.(removableCache)
	if !ok {
		return fmt.Errorf("unexpected cache type doesn't support Remove: %T", c)
	}
	// TODO(OREN) are we going to support dynamic watch for typed resources?
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	if err := rc.Remove(u); err != nil {
		return fmt.Errorf("removing %+v %w", gvk, err)
	}
	delete(wm.watchedKinds, gvk)
	return nil
}

// replaceWatches ensures all and only desired watches are running.
func (wm *Manager) replaceWatches() error {
	wm.watchedMux.Lock()
	defer wm.watchedMux.Unlock()

	desired := wm.managedKinds.Get()
	for gvk := range wm.watchedKinds {
		if _, ok := desired[gvk]; !ok {
			// TODO(OREN) aggregate errors instead of aborting
			if err := wm.doRemoveWatch(gvk); err != nil {
				return fmt.Errorf("removing watch for %+v %w", err)
			}
		}
	}

	for gvk := range desired {
		if _, ok := wm.watchedKinds[gvk]; !ok {
			// TODO(OREN) aggregate errors instead of aborting
			if err := wm.doAddWatch(gvk); err != nil {
				return fmt.Errorf("adding watch for %+v %w", err)
			}
		}
	}
	return nil
}

// OnAdd implements cache.ResourceEventHandler. Called by informers.
func (wm *Manager) OnAdd(obj interface{}) {
	// TODO(OREN) Distribute to registrar channels
}

// OnUpdate implements cache.ResourceEventHandler. Called by informers.
func (wm *Manager) OnUpdate(oldObj, newObj interface{}) {
	// TODO(OREN) Distribute to registrar channels
}

// OnDelete implements cache.ResourceEventHandler. Called by informers.
func (wm *Manager) OnDelete(obj interface{}) {
	// TODO(OREN) Distribute to registrar channels
}
