/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/event"

	opa "github.com/open-policy-agent/frameworks/constraint/pkg/client"
	configv1alpha1 "github.com/open-policy-agent/gatekeeper/api/v1alpha1"
	syncc "github.com/open-policy-agent/gatekeeper/pkg/controller/sync"
	"github.com/open-policy-agent/gatekeeper/pkg/target"
	"github.com/open-policy-agent/gatekeeper/pkg/util"
	"github.com/open-policy-agent/gatekeeper/pkg/watch"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// TODO write a reconciliation process that looks at the state of the cluster to make sure
// allFinalizers agrees with reality and launches a cleaner if the config is missing.

const (
	ctrlName      = "config-controller"
	finalizerName = "finalizers.gatekeeper.sh/config"
)

var CfgKey = types.NamespacedName{Namespace: util.GetNamespace(), Name: "config"}
var log = logf.Log.WithName("controller").WithValues("kind", "Config")

type Adder struct {
	Opa          *opa.Client
	WatchManager *watch.Manager
}

// Add creates a new ConfigController and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func (a *Adder) Add(mgr manager.Manager) error {
	r, err := newReconciler(mgr, a.Opa, a.WatchManager)
	if err != nil {
		return err
	}

	// Wipe cache on start - this is to allow for the future possibility that the OPA cache is stored remotely
	if _, err := a.Opa.RemoveData(context.Background(), target.WipeData{}); err != nil {
		return err
	}

	return add(mgr, r)
}

func (a *Adder) InjectOpa(o *opa.Client) {
	a.Opa = o
}

func (a *Adder) InjectWatchManager(wm *watch.Manager) {
	a.WatchManager = wm
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, opa *opa.Client, wm *watch.Manager) (reconcile.Reconciler, error) {
	// Events will be used to receive events from dynamic watches registered
	// via the registrar below.
	events := make(chan event.GenericEvent, 1024)
	syncAdder := syncc.Adder{
		Opa:    opa,
		Events: events,
	}
	// Create subordinate controller - we will feed it events dynamically via watch
	if err := syncAdder.Add(mgr); err != nil {
		return nil, fmt.Errorf("registering sync controller: %w", err)
	}

	w, err := wm.NewRegistrar(
		ctrlName,
		events)
	if err != nil {
		return nil, err
	}
	return &ReconcileConfig{
		Client:  mgr.GetClient(),
		scheme:  mgr.GetScheme(),
		opa:     opa,
		watcher: w,
		watched: newSet(),
	}, nil
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New(ctrlName, mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to Config
	err = c.Watch(&source.Kind{Type: &configv1alpha1.Config{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcileConfig{}

// ReconcileConfig reconciles a Config object
type ReconcileConfig struct {
	client.Client
	scheme  *runtime.Scheme
	opa     *opa.Client
	watcher *watch.Registrar
	watched *watchSet
}

// +kubebuilder:rbac:groups=*,resources=*,verbs=get;list;watch
// +kubebuilder:rbac:groups=config.gatekeeper.sh,resources=configs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=config.gatekeeper.sh,resources=configs/status,verbs=get;update;patch

// Reconcile reads that state of the cluster for a Config object and makes changes based on the state read
// and what is in the Config.Spec
// Automatically generate RBAC rules to allow the Controller to read all things (for sync)
// update is needed for finalizers
func (r *ReconcileConfig) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	// Fetch the Config instance
	if request.NamespacedName != CfgKey {
		log.Info("Ignoring unsupported config name", "namespace", request.NamespacedName.Namespace, "name", request.NamespacedName.Name)
		return reconcile.Result{}, nil
	}
	exists := true
	instance := &configv1alpha1.Config{}
	err := r.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		// if config is not found, we should remove cached data
		if errors.IsNotFound(err) {
			exists = false
		} else {
			// Error reading the object - requeue the request.
			return reconcile.Result{}, err
		}
	}

	if exists {
		// Actively remove config finalizer. This should automatically remove
		// the finalizer over time even if state teardown didn't work correctly
		// after a deprecation period, all finalizer code can be removed.
		if hasFinalizer(instance) {
			removeFinalizer(instance)
			if err := r.Update(context.Background(), instance); err != nil {
				return reconcile.Result{}, err
			}
		}
	}

	newSyncOnly := newSet()
	// If the config is being deleted the user is saying they don't want to
	// sync anything
	if exists && instance.GetDeletionTimestamp().IsZero() {
		for _, entry := range instance.Spec.Sync.SyncOnly {
			gvk := schema.GroupVersionKind{Group: entry.Group, Version: entry.Version, Kind: entry.Kind}
			newSyncOnly.Add(gvk)
		}
	}
	if !r.watched.Equals(newSyncOnly) {
		// Wipe all data to avoid stale state
		// TODO(OREN)!!
		//err := r.watcher.Pause()
		//defer func() {
		//	if err = r.watcher.Unpause(); err != nil {
		//		log.Error(err, "while unpausing watcher")
		//	}
		//}()
		if err != nil {
			return reconcile.Result{}, err
		}
		if _, err := r.opa.RemoveData(context.Background(), target.WipeData{}); err != nil {
			return reconcile.Result{}, err
		}
	}

	if err := r.watcher.ReplaceWatch(newSyncOnly.Items()); err != nil {
		return reconcile.Result{}, err
	}

	if exists {
		log.Info("updating config resource", "obj", instance)
		if err := r.Update(context.Background(), instance); err != nil {
			return reconcile.Result{}, err
		}
	}
	r.watched.Replace(newSyncOnly)
	return reconcile.Result{}, nil
}

func containsString(s string, items []string) bool {
	for _, item := range items {
		if item == s {
			return true
		}
	}
	return false
}

func removeString(s string, items []string) []string {
	var rval []string
	for _, item := range items {
		if item != s {
			rval = append(rval, item)
		}
	}
	return rval
}

// TearDownState removes all finalizers
// written by the config controller
func TearDownState(c client.Client, finished chan struct{}) {
	defer close(finished)
	syncCfg := &configv1alpha1.Config{}
	if err := c.Get(context.Background(), CfgKey, syncCfg); err != nil {
		log.Error(err, "while retrieving sync config")
		return
	}
	cleanFn := func() (bool, error) {
		syncCfg := &configv1alpha1.Config{}
		if err := c.Get(context.Background(), CfgKey, syncCfg); err != nil {
			if errors.IsNotFound(err) {
				return true, nil
			}
			log.Error(err, "get error while removing finalizer from config")
			return false, nil
		}
		removeFinalizer(syncCfg)
		if err := c.Update(context.Background(), syncCfg); err != nil {
			if errors.IsNotFound(err) {
				return true, nil
			}
			log.Error(err, "write error while removing finalizer from config")
			return false, nil
		}
		return true, nil
	}
	if err := wait.ExponentialBackoff(wait.Backoff{
		Duration: 500 * time.Millisecond,
		Factor:   2,
		Jitter:   1,
		Steps:    10,
	}, cleanFn); err != nil {
		log.Error(err, "max retries for removal of config finalizer reached")
	}
}

func hasFinalizer(instance *configv1alpha1.Config) bool {
	return containsString(finalizerName, instance.GetFinalizers())
}

func removeFinalizer(instance *configv1alpha1.Config) {
	instance.SetFinalizers(removeString(finalizerName, instance.GetFinalizers()))
}

func newSet() *watchSet {
	return &watchSet{
		set: make(map[schema.GroupVersionKind]bool),
	}
}

type watchSet struct {
	mux sync.RWMutex
	set map[schema.GroupVersionKind]bool
}

func (w *watchSet) Size() int {
	w.mux.RLock()
	defer w.mux.RUnlock()
	return len(w.set)
}

func (w *watchSet) Items() []schema.GroupVersionKind {
	w.mux.RLock()
	defer w.mux.RUnlock()
	var r []schema.GroupVersionKind
	for k := range w.set {
		r = append(r, k)
	}
	return r
}

func (w *watchSet) String() string {
	gvks := w.Items()
	var strs []string
	for _, gvk := range gvks {
		strs = append(strs, gvk.String())
	}
	return fmt.Sprintf("[%s]", strings.Join(strs, ", "))
}

func (w *watchSet) Add(gvks ...schema.GroupVersionKind) {
	w.mux.Lock()
	defer w.mux.Unlock()
	for _, gvk := range gvks {
		w.set[gvk] = true
	}
}

func (w *watchSet) Remove(gvks ...schema.GroupVersionKind) {
	w.mux.Lock()
	defer w.mux.Unlock()
	for _, gvk := range gvks {
		delete(w.set, gvk)
	}
}

func (w *watchSet) Dump() map[schema.GroupVersionKind]bool {
	w.mux.RLock()
	defer w.mux.RUnlock()
	m := make(map[schema.GroupVersionKind]bool, len(w.set))
	for k, v := range w.set {
		m[k] = v
	}
	return m
}

func (w *watchSet) AddSet(other *watchSet) {
	s := other.Dump()
	w.mux.Lock()
	defer w.mux.Unlock()
	for k := range s {
		w.set[k] = true
	}
}

func (w *watchSet) RemoveSet(other *watchSet) {
	s := other.Dump()
	w.mux.Lock()
	defer w.mux.Unlock()
	for k := range s {
		delete(w.set, k)
	}
}

func (w *watchSet) Equals(other *watchSet) bool {
	w.mux.RLock()
	defer w.mux.RUnlock()
	other.mux.RLock()
	defer other.mux.RUnlock()
	return reflect.DeepEqual(w.set, other.set)
}

func (w *watchSet) Replace(other *watchSet) {
	w.mux.Lock()
	defer w.mux.Unlock()
	other.mux.RLock()
	defer other.mux.RUnlock()

	newSet := make(map[schema.GroupVersionKind]bool)
	for k, v := range other.set {
		newSet[k] = v
	}
	w.set = newSet
}
