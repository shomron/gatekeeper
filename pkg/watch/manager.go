package watch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"k8s.io/apimachinery/pkg/api/meta"

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

	// Events are passed internally from informer event handlers to handleEvents for distribution.
	events chan interface{}
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
		watchedKinds: make(vitalsByGVK),
		metrics:      metrics,
		events:       make(chan interface{}, 1024), // TODO(OREN)
	}
	wm.started.Store(false)
	wm.managedKinds.mgr = wm
	return wm, nil
}

func (wm *Manager) NewRegistrar(parent string, events chan<- event.GenericEvent) (*Registrar, error) {
	return wm.managedKinds.NewRegistrar(parent, events)
}

// Start runs the watch manager, processing events received from dynamic informers and distributing them
// to registrars.
func (wm *Manager) Start(done <-chan struct{}) error {
	wm.stopped = make(chan struct{})
	grp, ctx := errgroup.WithContext(context.Background())
	grp.Go(func() error {
		select {
		case <-ctx.Done():
		case <-done:
		}
		// Unblock any informer event handlers
		close(wm.stopped)
		return context.Canceled
	})
	grp.Go(func() error {
		wm.eventLoop(ctx.Done())
		return context.Canceled
	})
	_ = grp.Wait()
	return nil
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
	m := wm.managedKinds.Get() // Not a deadlock but beware if assumptions change...
	if _, ok := m[gvk]; !ok {
		return fmt.Errorf("could not mark %+v as managed", gvk)
	}

	// TODO(OREN) - should we support structured dynamic watches (using GetInformerForKind?)
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	informer, err := wm.mgr.GetCache().GetInformer(u)
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
	// Send event to eventLoop() for processing
	select {
	case wm.events <- obj:
	case <-wm.stopped:
	}
}

// OnUpdate implements cache.ResourceEventHandler. Called by informers.
func (wm *Manager) OnUpdate(oldObj, newObj interface{}) {
	// Send event to eventLoop() for processing
	select {
	case wm.events <- oldObj:
	case <-wm.stopped:
	}
	select {
	case wm.events <- newObj:
	case <-wm.stopped:
	}
}

// OnDelete implements cache.ResourceEventHandler. Called by informers.
func (wm *Manager) OnDelete(obj interface{}) {
	// Send event to eventLoop() for processing
	select {
	case wm.events <- obj:
	case <-wm.stopped:
	}
}

// eventLoop receives events from informer callbacks and distributes them to registrars.
func (wm *Manager) eventLoop(stop <-chan struct{}) {
	for {
		select {
		case e, ok := <-wm.events:
			if !ok {
				return
			}
			wm.distributeEvent(stop, e)
		case <-stop:
			return
		}
	}
}

// distributeEvent distributes a single event to all registrars listening for that resource kind.
func (wm *Manager) distributeEvent(stop <-chan struct{}, obj interface{}) {
	o, ok := obj.(runtime.Object)
	if !ok || o == nil {
		// Invalid object, drop it
		return
	}
	gvk := o.GetObjectKind().GroupVersionKind()
	acc, err := meta.Accessor(o)
	if err != nil {
		// Invalid object, drop it
		return
	}
	e := event.GenericEvent{
		Meta:   acc,
		Object: o,
	}

	// Critical lock section
	var watchers []chan<- event.GenericEvent
	func() {
		wm.watchedMux.Lock()
		defer wm.watchedMux.Unlock()

		r, ok := wm.watchedKinds[gvk]
		if !ok {
			// Nobody is watching, drop it
			return
		}

		// TODO(OREN) reduce allocations here
		watchers = make([]chan<- event.GenericEvent, 0, len(r.registrars))
		for w := range r.registrars {
			if w.events == nil {
				continue
			}
			watchers = append(watchers, w.events)
		}
	}()

	// Distribute the event
	for _, w := range watchers {
		select {
		case w <- e:
		// TODO(OREN) add timeout
		case <-stop:
		}
	}
}
