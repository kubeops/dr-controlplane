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

package ocm

import (
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"open-cluster-management.io/addon-framework/pkg/addonfactory"
	addonv1alpha1 "open-cluster-management.io/api/addon/v1alpha1"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
)

func testOptions() AgentOptions {
	opts := NewAgentOptions()
	opts.RegistryFQDN = "registry.example.com"
	opts.ImageRegistry = ""
	opts.ImageRepository = "dr-controlplane"
	opts.ImageTag = "v0.0.1-test"
	opts.ImagePullSecrets = []string{"regcred", "regcred-2"}
	return opts
}

// renderAgent renders the embedded chart the same way the addon manager does.
func renderAgent(t *testing.T, opts AgentOptions) []metav1.Object {
	t.Helper()

	addonAgent, err := addonfactory.NewAgentAddonFactory(AddonName, FS, "manifests/dr-controlplane-agent").
		WithScheme(scheme).
		WithAgentInstallNamespace(agentInstallNamespace(opts)).
		WithGetValuesFuncs(getValues(opts, nil, nil)).
		BuildHelmAgentAddon()
	if err != nil {
		t.Fatalf("build agent addon: %v", err)
	}

	cluster := &clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{Name: "dc-a"}}
	cluster.Status.Version.Kubernetes = "v1.30.0"
	// The ManagedClusterAddOn CRD always defaults InstallNamespace, which is why
	// the addon used to install into the wrong namespace.
	mca := &addonv1alpha1.ManagedClusterAddOn{
		ObjectMeta: metav1.ObjectMeta{Name: AddonName, Namespace: "dc-a"},
		Spec: addonv1alpha1.ManagedClusterAddOnSpec{
			InstallNamespace: addonfactory.AddonDefaultInstallNamespace,
		},
	}

	objects, err := addonAgent.Manifests(cluster, mca)
	if err != nil {
		t.Fatalf("render manifests: %v", err)
	}
	if len(objects) == 0 {
		t.Fatal("no objects rendered")
	}

	out := make([]metav1.Object, 0, len(objects))
	for _, obj := range objects {
		acc, err := meta.Accessor(obj)
		if err != nil {
			t.Fatalf("accessor: %v", err)
		}
		if kind, err := meta.TypeAccessor(obj); err == nil && kind.GetKind() == "Namespace" {
			t.Errorf("chart emitted a Namespace object; the addon must not own the shared %s namespace", opts.InstallNamespace)
		}
		out = append(out, acc)
	}
	return out
}

// The agent must land in the namespace that holds the coordination credential,
// not the addon-framework default, even though the CRD always populates
// Spec.InstallNamespace.
func TestAgentManifestsUseConfiguredInstallNamespace(t *testing.T) {
	opts := testOptions()
	for _, obj := range renderAgent(t, opts) {
		if obj.GetNamespace() != opts.InstallNamespace {
			t.Errorf("object %q namespace = %q, want %q", obj.GetName(), obj.GetNamespace(), opts.InstallNamespace)
		}
	}
}

// Image, pull secrets and the Lease timings must all come from the addon
// manager's options rather than the chart's hardcoded defaults.
func TestAgentDeploymentCarriesInjectedValues(t *testing.T) {
	opts := testOptions()

	var deploy *appsv1.Deployment
	for _, obj := range renderAgent(t, opts) {
		if d, ok := obj.(*appsv1.Deployment); ok {
			deploy = d
		}
	}
	if deploy == nil {
		t.Fatal("no Deployment rendered")
	}

	pod := deploy.Spec.Template.Spec
	if len(pod.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(pod.Containers))
	}
	if want := "registry.example.com/dr-controlplane:v0.0.1-test"; pod.Containers[0].Image != want {
		t.Errorf("image = %q, want %q", pod.Containers[0].Image, want)
	}
	secrets := make([]string, 0, len(pod.ImagePullSecrets))
	for _, s := range pod.ImagePullSecrets {
		secrets = append(secrets, s.Name)
	}
	if strings.Join(secrets, ",") != "regcred,regcred-2" {
		t.Errorf("imagePullSecrets = %v, want [regcred regcred-2]", secrets)
	}

	args := strings.Join(pod.Containers[0].Args, " ")
	for _, want := range []string{
		"--dc-name=dc-a",
		"--namespace=dc-failover",
		"--election-lease-duration=45s",
		"--election-renew-deadline=40s",
		"--election-retry-period=3s",
		"--holder-priority-window=20s",
		"--v=2",
		"--health-lease-duration=15s",
		"--health-renew-interval=5s",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("agent args %q missing %q", args, want)
		}
	}
}

// An election lease at or below the data plane marker fence TTL would let two
// data centers be writable at once, so the addon manager must refuse to start.
func TestValidateRejectsElectionLeaseInsideFenceTTL(t *testing.T) {
	opts := NewAgentOptions()
	if err := opts.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
	if opts.ElectionLeaseDuration <= markerFenceTTL {
		t.Fatalf("default election lease %s must exceed the %s fence TTL", opts.ElectionLeaseDuration, markerFenceTTL)
	}

	for _, d := range []time.Duration{15 * time.Second, markerFenceTTL} {
		bad := NewAgentOptions()
		bad.ElectionLeaseDuration = d
		bad.ElectionRenewDeadline = d - 5*time.Second
		bad.ElectionRetryPeriod = 2 * time.Second
		if err := bad.Validate(); err == nil {
			t.Errorf("election lease duration %s accepted, want rejected", d)
		}
	}
}

func renderDeployment(t *testing.T, opts AgentOptions) (*appsv1.Deployment, *policyv1.PodDisruptionBudget) {
	t.Helper()
	var deploy *appsv1.Deployment
	var pdb *policyv1.PodDisruptionBudget
	for _, obj := range renderAgent(t, opts) {
		switch o := obj.(type) {
		case *appsv1.Deployment:
			deploy = o
		case *policyv1.PodDisruptionBudget:
			pdb = o
		}
	}
	if deploy == nil {
		t.Fatal("no Deployment rendered")
	}
	return deploy, pdb
}

func envValue(c corev1.Container, name string) (corev1.EnvVar, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e, true
		}
	}
	return corev1.EnvVar{}, false
}

// The false-failover hardening must reach every rendered agent: a hot standby,
// surge-only rollouts, a PDB, a priority class, memory request = limit with no
// CPU limit, the pod name as writer identity, and fast HTTP/2 dead-connection
// detection.
func TestAgentDeploymentFalseFailoverHardening(t *testing.T) {
	deploy, pdb := renderDeployment(t, testOptions())

	if got := *deploy.Spec.Replicas; got != 2 {
		t.Errorf("replicas = %d, want 2", got)
	}
	ru := deploy.Spec.Strategy.RollingUpdate
	if ru == nil || ru.MaxUnavailable.IntValue() != 0 || ru.MaxSurge.IntValue() != 1 {
		t.Errorf("rollout must be surge only (maxUnavailable 0, maxSurge 1), got %+v", ru)
	}
	if pdb == nil {
		t.Fatal("no PodDisruptionBudget rendered with 2 replicas")
	}
	if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.IntValue() != 1 {
		t.Errorf("PDB minAvailable = %v, want 1", pdb.Spec.MinAvailable)
	}

	pod := deploy.Spec.Template.Spec
	if pod.PriorityClassName != "system-cluster-critical" {
		t.Errorf("priorityClassName = %q, want system-cluster-critical", pod.PriorityClassName)
	}
	c := pod.Containers[0]
	req, lim := c.Resources.Requests[corev1.ResourceMemory], c.Resources.Limits[corev1.ResourceMemory]
	if req.IsZero() || req.Cmp(lim) != 0 {
		t.Errorf("memory request %v must equal the limit %v", req.String(), lim.String())
	}
	if want := resource.MustParse("256Mi"); lim.Cmp(want) != 0 {
		t.Errorf("memory limit = %v, want 256Mi", lim.String())
	}
	if _, ok := c.Resources.Limits[corev1.ResourceCPU]; ok {
		t.Error("the agent must have no CPU limit: throttling stalls Lease renewals")
	}

	pn, ok := envValue(c, "POD_NAME")
	if !ok || pn.ValueFrom == nil || pn.ValueFrom.FieldRef == nil || pn.ValueFrom.FieldRef.FieldPath != "metadata.name" {
		t.Errorf("POD_NAME must come from metadata.name, got %+v", pn)
	}
	for name, want := range map[string]string{
		"HTTP2_READ_IDLE_TIMEOUT_SECONDS": "10",
		"HTTP2_PING_TIMEOUT_SECONDS":      "5",
	} {
		if e, ok := envValue(c, name); !ok || e.Value != want {
			t.Errorf("%s = %q, want %q", name, e.Value, want)
		}
	}
}

// One replica must not render a PDB: minAvailable 1 would block every drain.
func TestSingleReplicaRendersNoPDB(t *testing.T) {
	opts := testOptions()
	opts.Replicas = 1
	opts.PriorityClassName = ""
	deploy, pdb := renderDeployment(t, opts)
	if pdb != nil {
		t.Error("a single replica must not get a PDB")
	}
	if deploy.Spec.Template.Spec.PriorityClassName != "" {
		t.Error("an empty priority class must leave the field unset")
	}
}
