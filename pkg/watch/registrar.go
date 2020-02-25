package watch

import (
	"fmt"
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/event"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

type vitals struct {
	gvk        schema.GroupVersionKind
	registrars map[*Registrar]bool
}

type vitalsByGVK map[schema.GroupVersionKind]vitals

func (w *vitals) merge(wv vitals) vitals {
	if w == nil {
		return wv
	}
	registrars := make(map[*Registrar]bool)
	for r := range w.registrars {
		registrars[r] = true
	}
	for r := range wv.registrars {
		registrars[r] = true
	}
	return vitals{
		gvk:        w.gvk,
		registrars: registrars,
	}
}

// recordKeeper holds the source of truth for the intended state of the manager
// This is essentially a read/write lock on the wrapped map (the `intent` variable)
type recordKeeper struct {
	// map[registrarName][kind]
	intent     map[string]vitalsByGVK
	intentMux  sync.RWMutex
	registrars map[string]*Registrar
	mgr        *Manager
}

func (r *recordKeeper) NewRegistrar(parentName string, events chan<- event.GenericEvent) (*Registrar, error) {
	r.intentMux.Lock()
	defer r.intentMux.Unlock()
	if _, ok := r.registrars[parentName]; ok {
		return nil, fmt.Errorf("registrar for %s already exists", parentName)
	}
	out := &Registrar{
		parentName:   parentName,
		mgr:          r.mgr,
		managedKinds: r,
		events:       events,
	}
	r.registrars[parentName] = out
	return out, nil
}

func (r *recordKeeper) Update(parentName string, m vitalsByGVK) {
	r.intentMux.Lock()
	defer r.intentMux.Unlock()
	if _, ok := r.intent[parentName]; !ok {
		r.intent[parentName] = make(vitalsByGVK)
	}
	for gvk, v := range m {
		r.intent[parentName][gvk] = v
	}
}

func (r *recordKeeper) ReplaceRegistrarRoster(reg *Registrar, roster map[schema.GroupVersionKind]vitals) {
	r.intentMux.Lock()
	defer r.intentMux.Unlock()
	r.intent[reg.parentName] = roster
}

func (r *recordKeeper) Remove(parentName string, gvk schema.GroupVersionKind) {
	r.intentMux.Lock()
	defer r.intentMux.Unlock()
	delete(r.intent[parentName], gvk)
}

// Get returns all managed vitals, merged across registrars.
func (r *recordKeeper) Get() vitalsByGVK {
	r.intentMux.RLock()
	defer r.intentMux.RUnlock()
	cpy := make(map[string]vitalsByGVK)
	for k := range r.intent {
		cpy[k] = make(vitalsByGVK)
		for k2, v := range r.intent[k] {
			cpy[k][k2] = v
		}
	}
	managedKinds := make(vitalsByGVK)
	for _, registrar := range cpy {
		for gvk, v := range registrar {
			if mk, ok := managedKinds[gvk]; ok {
				merged := mk.merge(v)
				managedKinds[gvk] = merged
			} else {
				managedKinds[gvk] = v
			}
		}
	}
	return managedKinds
}

// GetGVK returns all managed kinds, merged across registrars.
func (r *recordKeeper) GetGVK() []schema.GroupVersionKind {
	var gvks []schema.GroupVersionKind

	g := r.Get()
	for gvk := range g {
		gvks = append(gvks, gvk)
	}
	return gvks
}

func newRecordKeeper() *recordKeeper {
	return &recordKeeper{
		intent:     make(map[string]vitalsByGVK),
		registrars: make(map[string]*Registrar),
	}
}

// A Registrar allows a parent to add/remove child watches
type Registrar struct {
	parentName   string
	mgr          *Manager
	managedKinds *recordKeeper
	events       chan<- event.GenericEvent
}

// AddWatch registers a watch for the given kind
func (r *Registrar) AddWatch(gvk schema.GroupVersionKind) error {
	wv := vitals{
		gvk:        gvk,
		registrars: map[*Registrar]bool{r: true},
	}
	r.managedKinds.Update(r.parentName, vitalsByGVK{gvk: wv})
	return r.mgr.addWatch(r, gvk)
}

func (r *Registrar) ReplaceWatch(gvks []schema.GroupVersionKind) error {
	roster := make(vitalsByGVK)
	for _, gvk := range gvks {
		wv := vitals{
			gvk:        gvk,
			registrars: map[*Registrar]bool{r: true},
		}
		roster[gvk] = wv
	}
	r.managedKinds.ReplaceRegistrarRoster(r, roster)
	return r.mgr.replaceWatches(r)
}

func (r *Registrar) RemoveWatch(gvk schema.GroupVersionKind) error {
	r.managedKinds.Remove(r.parentName, gvk)
	return r.mgr.removeWatch(gvk)
}
