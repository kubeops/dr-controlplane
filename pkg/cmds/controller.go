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

package cmds

import (
	"open-cluster-management.io/dr-controlplane/pkg/leases"
	"open-cluster-management.io/dr-controlplane/pkg/topology"

	"github.com/spf13/cobra"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

func newCmdController() *cobra.Command {
	var (
		hubKubeconfig   string
		coordKubeconfig string
		namespace       = leases.DefaultNamespace
		requireSpread   = true
		regions         = map[string]string{}
	)
	cmd := &cobra.Command{
		Use:               "controller",
		Short:             "Run the topology controller (ensures one primary DC Lease per scope)",
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			hubCfg, err := restConfig(hubKubeconfig)
			if err != nil {
				return err
			}
			hub, err := dynamic.NewForConfig(hubCfg)
			if err != nil {
				return err
			}

			coordPath := coordKubeconfig
			if coordPath == "" {
				coordPath = hubKubeconfig
			}
			coordCfg, err := restConfig(coordPath)
			if err != nil {
				return err
			}
			coord, err := kubernetes.NewForConfig(coordCfg)
			if err != nil {
				return err
			}

			regionOf := func(cluster string) string {
				if r, ok := regions[cluster]; ok {
					return r
				}
				return cluster
			}
			return topology.New(hub, coord, namespace, regionOf, requireSpread).Run(cmd.Context())
		},
	}
	fs := cmd.Flags()
	fs.StringVar(&hubKubeconfig, "hub-kubeconfig", hubKubeconfig, "Path to the OCM hub kubeconfig (reads PlacementPolicies). Empty means in cluster.")
	fs.StringVar(&coordKubeconfig, "coord-kubeconfig", coordKubeconfig, "Path to the coordination control plane kubeconfig (writes Leases). Empty falls back to the hub config.")
	fs.StringVar(&namespace, "namespace", namespace, "Namespace for the coordination Leases.")
	fs.BoolVar(&requireSpread, "require-spread", requireSpread, "Refuse to manage a scope that does not span at least three failure domains.")
	fs.StringToStringVar(&regions, "region", regions, "Cluster to region mapping for spread validation, for example dc-a=us-east,dc-b=us-west.")
	return cmd
}
