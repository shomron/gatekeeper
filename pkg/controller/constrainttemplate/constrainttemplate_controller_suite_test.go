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

package constrainttemplate

import (
	stdlog "log"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/onsi/gomega"
	"github.com/open-policy-agent/gatekeeper/api"
	"github.com/open-policy-agent/gatekeeper/pkg/util"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var cfg *rest.Config

func TestMain(m *testing.M) {
	var err error

	// Load framework CRDs separately - controller-runtime tries to parse any *.yaml it sees,
	// picks up kustomization.yaml, and fails. TODO upstream better filtering support for envtest.
	frameworkCRDs, err := util.ReadCRD(filepath.Join("..", "..", "..", "vendor", "github.com", "open-policy-agent", "frameworks", "constraint", "deploy", "crds.yaml"))
	if err != nil {
		stdlog.Fatal(err)
	}

	t := &envtest.Environment{
		CRDDirectoryPaths: []string{
			// Due to a bug in how the testenv loads CRDDirectoryPaths (files slice
			// defined at too-wide a scope), this path needs to come first.
			// (See controller-runtime comment bug above) filepath.Join("..", "..", "vendor", "github.com", "open-policy-agent", "frameworks", "constraint", "deploy"),
			filepath.Join("..", "..", "..", "config", "crd", "bases"),
		},
		CRDs:                  frameworkCRDs,
		ErrorIfCRDPathMissing: true,
	}
	if err := api.AddToScheme(scheme.Scheme); err != nil {
		stdlog.Fatal(err)
	}

	if cfg, err = t.Start(); err != nil {
		stdlog.Fatal(err)
	}
	stdlog.Print("STARTED")

	code := m.Run()
	if err = t.Stop(); err != nil {
		stdlog.Printf("error while trying to stop server: %v", err)
	}
	os.Exit(code)
}

// SetupTestReconcile returns a reconcile.Reconcile implementation that delegates to inner and
// writes the request to requests after Reconcile is finished.
func SetupTestReconcile(inner reconcile.Reconciler) (reconcile.Reconciler, chan reconcile.Request) {
	requests := make(chan reconcile.Request)
	fn := reconcile.Func(func(req reconcile.Request) (reconcile.Result, error) {
		result, err := inner.Reconcile(req)
		requests <- req
		return result, err
	})
	return fn, requests
}

// StartTestManager adds recFn
func StartTestManager(mgr manager.Manager, g *gomega.GomegaWithT) (chan struct{}, *sync.WaitGroup) {
	stop := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		g.Expect(mgr.Start(stop)).NotTo(gomega.HaveOccurred())
	}()
	return stop, wg
}
