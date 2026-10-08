/*
Copyright AppsCode Inc. and Contributors.

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

package failovergroup

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"kubeops.dev/dr-controlplane/pkg/leases"
	appsv1 "kubeops.dev/petset/apis/apps/v1"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	kmapi "kmodules.xyz/client-go/api/v1"
)

// FailoverGroupGVR is the cluster scoped FailoverGroup resource on the hub.
var FailoverGroupGVR = schema.GroupVersionResource{
	Group:    appsv1.GroupName,
	Version:  "v1",
	Resource: appsv1.ResourceFailoverGroups,
}

const (
	// fieldManager owns the status fields this controller writes. Members are
	// owned by the engine operators' own field managers and never touched here.
	fieldManager = "dr-controlplane-failovergroup"

	// AnnSwitchoverTo is the engine contract for a planned, coordinated switchover
	// of one database: the engine operator quiesces, waits for the target to catch
	// up, and hands the primary DC Lease over. Every DC-DR engine honors it.
	AnnSwitchoverTo = "dr.kubedb.com/switchover-to"

	// ConditionTypeReady is True when the group is Steady on its target DC.
	ConditionTypeReady = "Ready"
)

// Controller reconciles FailoverGroup status and the follow-dc annotation of
// dependent groups' Leases.
type Controller struct {
	hub      dynamic.Interface
	coord    kubernetes.Interface
	mapper   meta.RESTMapper
	ns       string
	interval time.Duration
}

// New builds a FailoverGroup Controller. hub reads FailoverGroups and member
// workloads, coord reads and annotates the primary DC Leases, mapper resolves
// member kinds to resources.
func New(hub dynamic.Interface, coord kubernetes.Interface, mapper meta.RESTMapper, ns string) *Controller {
	if ns == "" {
		ns = leases.DefaultNamespace
	}
	return &Controller{hub: hub, coord: coord, mapper: mapper, ns: ns, interval: 5 * time.Second}
}

// Run reconciles on an interval until ctx is done.
func (c *Controller) Run(ctx context.Context) error {
	klog.InfoS("failover group controller running", "namespace", c.ns, "interval", c.interval.String())
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		c.reconcile(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Controller) reconcile(ctx context.Context) {
	list, err := c.hub.Resource(FailoverGroupGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.ErrorS(err, "failed to list FailoverGroups")
		return
	}
	groups := make(map[string]*appsv1.FailoverGroup, len(list.Items))
	for i := range list.Items {
		g := &appsv1.FailoverGroup{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(list.Items[i].Object, g); err != nil {
			klog.ErrorS(err, "failed to convert FailoverGroup", "name", list.Items[i].GetName())
			continue
		}
		groups[g.Name] = g
	}

	now := time.Now()
	for _, g := range groups {
		leaseName := leases.GroupScope(g.Name).PrimaryLeaseName()
		lease, view, err := c.leaseView(ctx, leaseName, now)
		if err != nil {
			klog.ErrorS(err, "failed to read the group's primary DC Lease", "group", g.Name, "lease", leaseName)
			continue
		}
		d := Decide(g, groups, view, now)

		if lease != nil && d.FollowDC != view.FollowDC {
			if err := c.setFollowDC(ctx, leaseName, d.FollowDC); err != nil {
				klog.ErrorS(err, "failed to update follow-dc on the group's Lease", "group", g.Name, "lease", leaseName, "followDC", d.FollowDC)
			}
		}
		if d.SwitchoverTo != "" {
			c.requestSwitchover(ctx, g, d.SwitchoverTo)
		}
		if err := c.applyStatus(ctx, g, leaseName, view.Holder, d, now); err != nil {
			klog.ErrorS(err, "failed to update FailoverGroup status", "group", g.Name)
		}
	}

	c.clearStaleFollowDC(ctx, groups)
}

// leaseView reads a group's primary DC Lease and, when it has a holder, that
// DC's health Lease. A missing Lease is not an error.
func (c *Controller) leaseView(ctx context.Context, name string, now time.Time) (*coordinationv1.Lease, LeaseView, error) {
	l, err := c.coord.CoordinationV1().Leases(c.ns).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, LeaseView{}, nil
	}
	if err != nil {
		return nil, LeaseView{}, err
	}
	v := LeaseView{
		Exists:    true,
		MemberDCs: memberList(l.Annotations[leases.AnnMemberDCs]),
		FollowDC:  l.Annotations[leases.AnnFollowDC],
	}
	if l.Spec.HolderIdentity != nil {
		v.Holder = *l.Spec.HolderIdentity
	}
	if v.Holder != "" && leaseFresh(l, now) {
		h, err := c.coord.CoordinationV1().Leases(c.ns).Get(ctx, leases.HealthLeaseName(v.Holder), metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, LeaseView{}, err
		}
		v.HolderAlive = err == nil && leaseFresh(h, now)
	}
	return l, v, nil
}

func memberList(v string) []string {
	set := leases.ParseMemberDCs(v)
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

func leaseFresh(l *coordinationv1.Lease, now time.Time) bool {
	if l == nil || l.Spec.RenewTime == nil || l.Spec.LeaseDurationSeconds == nil {
		return false
	}
	return now.Before(l.Spec.RenewTime.Add(time.Duration(*l.Spec.LeaseDurationSeconds) * time.Second))
}

// setFollowDC sets or (for an empty dc) removes the follow-dc annotation.
func (c *Controller) setFollowDC(ctx context.Context, leaseName, dc string) error {
	var v any = dc
	if dc == "" {
		v = nil
	}
	raw, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]any{leases.AnnFollowDC: v}}})
	if err != nil {
		return err
	}
	if _, err := c.coord.CoordinationV1().Leases(c.ns).Patch(ctx, leaseName, types.MergePatchType, raw, metav1.PatchOptions{}); err != nil {
		return err
	}
	klog.InfoS("updated follow-dc on a dependent group's Lease", "lease", leaseName, "followDC", dc)
	return nil
}

// clearStaleFollowDC removes follow-dc from Leases whose group no longer exists or
// no longer has dependencies, so a deleted FailoverGroup can never keep a Lease
// pinned.
func (c *Controller) clearStaleFollowDC(ctx context.Context, groups map[string]*appsv1.FailoverGroup) {
	list, err := c.coord.CoordinationV1().Leases(c.ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.ErrorS(err, "failed to list Leases for follow-dc cleanup")
		return
	}
	for i := range list.Items {
		l := &list.Items[i]
		if l.Annotations[leases.AnnFollowDC] == "" {
			continue
		}
		scope, ok := leases.ScopeFromPrimaryLeaseName(l.Name)
		if !ok {
			continue
		}
		if g, exists := groups[scope.Group]; exists && !scope.IsGlobal() && len(g.Spec.DependsOn) > 0 {
			continue
		}
		if err := c.setFollowDC(ctx, l.Name, ""); err != nil {
			klog.ErrorS(err, "failed to clear stale follow-dc", "lease", l.Name)
		}
	}
}

// requestSwitchover asks every member of g to switch over to dc through the
// engine's switchover-to annotation. A member already carrying a switchover-to
// (ours, or a human's) is left alone.
func (c *Controller) requestSwitchover(ctx context.Context, g *appsv1.FailoverGroup, dc string) {
	for i := range g.Status.Members {
		m := &g.Status.Members[i]
		mapping, err := c.mapper.RESTMapping(schema.GroupKind{Group: m.APIGroup, Kind: m.Kind})
		if err != nil {
			klog.ErrorS(err, "failed to resolve a FailoverGroup member's resource", "group", g.Name, "member", m.Kind+"/"+m.Namespace+"/"+m.Name)
			continue
		}
		ri := c.hub.Resource(mapping.Resource).Namespace(m.Namespace)
		obj, err := ri.Get(ctx, m.Name, metav1.GetOptions{})
		if err != nil {
			klog.ErrorS(err, "failed to read a FailoverGroup member", "group", g.Name, "member", m.Kind+"/"+m.Namespace+"/"+m.Name)
			continue
		}
		if obj.GetAnnotations()[AnnSwitchoverTo] != "" {
			continue
		}
		raw, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]string{AnnSwitchoverTo: dc}}})
		if err != nil {
			continue
		}
		if _, err := ri.Patch(ctx, m.Name, types.MergePatchType, raw, metav1.PatchOptions{}); err != nil {
			klog.ErrorS(err, "failed to request a member switchover", "group", g.Name, "member", m.Kind+"/"+m.Namespace+"/"+m.Name, "to", dc)
			continue
		}
		klog.InfoS("requested a member switchover to follow the group's dependencies", "group", g.Name, "member", m.Kind+"/"+m.Namespace+"/"+m.Name, "to", dc)
	}
}

// applyStatus server side applies the fields this controller owns.
func (c *Controller) applyStatus(ctx context.Context, g *appsv1.FailoverGroup, leaseName, holder string, d Decision, now time.Time) error {
	lastTransition := g.Status.LastTransitionTime
	if d.ActiveDC != g.Status.ActiveDC {
		t := metav1.NewTime(now)
		lastTransition = &t
	}

	cond := kmapi.Condition{
		Type:               ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: g.Generation,
		Reason:             string(d.Phase),
		Message:            d.Message,
		LastTransitionTime: metav1.NewTime(now),
	}
	if d.Phase == appsv1.FailoverGroupPhaseSteady {
		cond.Status = metav1.ConditionTrue
	}
	for _, old := range g.Status.Conditions {
		if old.Type == cond.Type && old.Status == cond.Status {
			cond.LastTransitionTime = old.LastTransitionTime
		}
	}

	status := appsv1.FailoverGroupStatus{
		ObservedGeneration: g.Generation,
		LeaseName:          leaseName,
		LeaseHolder:        holder,
		ActiveDC:           d.ActiveDC,
		Phase:              d.Phase,
		LastTransitionTime: lastTransition,
		Conditions:         []kmapi.Condition{cond},
	}
	st, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&status)
	if err != nil {
		return err
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": appsv1.SchemeGroupVersion.String(),
		"kind":       appsv1.ResourceKindFailoverGroup,
		"metadata":   map[string]any{"name": g.Name},
		"status":     st,
	}}
	_, err = c.hub.Resource(FailoverGroupGVR).ApplyStatus(ctx, g.Name, obj, metav1.ApplyOptions{FieldManager: fieldManager, Force: true})
	return err
}
