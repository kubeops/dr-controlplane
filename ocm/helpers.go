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

package ocm

import (
	"os"

	"gomodules.xyz/cert/certstore"
	"k8s.io/client-go/rest"
	"open-cluster-management.io/addon-framework/pkg/addonfactory"
	"open-cluster-management.io/api/addon/v1alpha1"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
)

const (
	// defaultCoordKubeconfigSecret is the Secret the agent mounts for coordination
	// apiserver auth. It can be overridden with COORD_KUBECONFIG_SECRET.
	defaultCoordKubeconfigSecret = "coord-kubeconfig"
)

// coordKubeconfigSecret returns the coordination kubeconfig Secret name passed to
// the agent chart. An empty value disables the mount (in cluster config is used).
func coordKubeconfigSecret() string {
	return os.Getenv("COORD_KUBECONFIG_SECRET")
}

// getValues injects per cluster values into the embedded agent chart. It keeps
// only what the scaffold needs: the ManagedCluster name becomes the agent's
// --dc-name, plus the coordination namespace and the coord-kubeconfig Secret
// name pass through unchanged. The kubeslice Cluster CR bootstrap is dropped.
//
// restConfig and cs are threaded through for the follow up CSR work that will
// replace coord-kubeconfig; they are not consumed by the scaffold values.
func getValues(_ *rest.Config, _ *certstore.CertStore) addonfactory.GetValuesFunc {
	return func(cluster *clusterv1.ManagedCluster, addon *v1alpha1.ManagedClusterAddOn) (addonfactory.Values, error) {
		ns := defaultControlPlaneNamespace
		if addon != nil && addon.Spec.InstallNamespace != "" {
			ns = addon.Spec.InstallNamespace
		}

		coordSecret := coordKubeconfigSecret()
		if coordSecret == "" {
			coordSecret = defaultCoordKubeconfigSecret
		}

		values := addonfactory.Values{
			// clusterName drives the agent Deployment's --dc-name flag.
			"clusterName": cluster.Name,
			"namespace":   ns,
			"agent": map[string]interface{}{
				"coordKubeconfigSecret": coordSecret,
			},
		}
		return values, nil
	}
}
