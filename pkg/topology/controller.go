/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Free Trial License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Free-Trial-1.0.0.md

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topology

import (
	"context"
	"encoding/json"
	"time"

	"open-cluster-management.io/dr-controlplane/pkg/leases"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	appsv1 "kubeops.dev/petset/apis/apps/v1"
)

// PlacementPolicyGVR is the cluster scoped PlacementPolicy resource on the hub.
var PlacementPolicyGVR = schema.GroupVersionResource{
	Group:    appsv1.GroupName,
	Version:  "v1",
	Resource: "placementpolicies",
}

// Controller watches PlacementPolicy on the hub and ensures a primary DC Lease
// for each trigger scope on the coordination control plane.
type Controller struct {
	hub      dynamic.Interface
	coord    kubernetes.Interface
	ns       string
	resync   time.Duration
	regionOf func(string) string
	// requireSpread refuses to manage a scope that does not span >= 3 failure domains.
	requireSpread bool
}

// New builds a topology Controller. hub reads PlacementPolicies, coord writes Leases.
func New(hub dynamic.Interface, coord kubernetes.Interface, ns string, regionOf func(string) string, requireSpread bool) *Controller {
	if ns == "" {
		ns = leases.DefaultNamespace
	}
	return &Controller{
		hub:           hub,
		coord:         coord,
		ns:            ns,
		resync:        2 * time.Minute,
		regionOf:      regionOf,
		requireSpread: requireSpread,
	}
}

// Run starts the informer and reconcile loop until ctx is done.
func (c *Controller) Run(ctx context.Context) error {
	factory := dynamicinformer.NewDynamicSharedInformerFactory(c.hub, c.resync)
	gi := factory.ForResource(PlacementPolicyGVR)
	informer := gi.Informer()
	lister := gi.Lister()

	trigger := make(chan struct{}, 1)
	enqueue := func(any) {
		select {
		case trigger <- struct{}{}:
		default:
		}
	}
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    enqueue,
		UpdateFunc: func(_, obj any) { enqueue(obj) },
		DeleteFunc: enqueue,
	})

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return ctx.Err()
	}
	klog.InfoS("topology controller running", "namespace", c.ns)

	ticker := time.NewTicker(c.resync)
	defer ticker.Stop()
	c.reconcile(ctx, lister)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-trigger:
			c.reconcile(ctx, lister)
		case <-ticker.C:
			c.reconcile(ctx, lister)
		}
	}
}

func (c *Controller) reconcile(ctx context.Context, lister cache.GenericLister) {
	objs, err := lister.List(labels.Everything())
	if err != nil {
		klog.ErrorS(err, "failed to list PlacementPolicies")
		return
	}
	pps := make([]*appsv1.PlacementPolicy, 0, len(objs))
	for _, o := range objs {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		pp := &appsv1.PlacementPolicy{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, pp); err != nil {
			klog.ErrorS(err, "failed to convert PlacementPolicy", "name", u.GetName())
			continue
		}
		pps = append(pps, pp)
	}

	topo, errs := Derive(pps)
	for _, e := range errs {
		klog.ErrorS(e, "skipping invalid PlacementPolicy failover config")
	}
	for name, st := range topo.Scopes {
		if c.requireSpread {
			if err := st.ValidateSpread(c.regionOf); err != nil {
				klog.ErrorS(err, "refusing to manage scope without sufficient failure domain spread", "lease", name)
				continue
			}
		}
		if err := c.ensureLease(ctx, name, st); err != nil {
			klog.ErrorS(err, "failed to ensure primary DC Lease", "lease", name)
		}
	}
}

// ensureLease creates or annotates the primary DC Lease without touching the
// spec the agents manage (holderIdentity, renewTime).
func (c *Controller) ensureLease(ctx context.Context, name string, st *ScopeTopology) error {
	ann := map[string]string{
		leases.AnnMemberDCs: leases.FormatMemberDCs(st.Members),
		leases.AnnScope:     ScopeAnnotationValue(st.Scope),
	}
	cl := c.coord.CoordinationV1().Leases(c.ns)

	cur, err := cl.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		l := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   c.ns,
				Labels:      map[string]string{leases.LabelManagedBy: leases.ValueManagedBy},
				Annotations: ann,
			},
		}
		if _, err := cl.Create(ctx, l, metav1.CreateOptions{}); err != nil {
			return err
		}
		klog.InfoS("created primary DC Lease", "lease", name, "members", ann[leases.AnnMemberDCs])
		return nil
	}
	if err != nil {
		return err
	}

	if annotationsContain(cur.Annotations, ann) {
		return nil
	}
	patch := map[string]any{"metadata": map[string]any{"annotations": ann}}
	raw, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	if _, err := cl.Patch(ctx, name, types.MergePatchType, raw, metav1.PatchOptions{}); err != nil {
		return err
	}
	klog.InfoS("updated primary DC Lease members", "lease", name, "members", ann[leases.AnnMemberDCs])
	return nil
}

func annotationsContain(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}
