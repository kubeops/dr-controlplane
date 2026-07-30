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
	"open-cluster-management.io/dr-controlplane/ocm"

	"github.com/spf13/cobra"
)

func newCmdAddonManager() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "addon-manager",
		Short: "Run the OCM addon manager (installs the dr-controlplane agent onto ManagedClusters)",
		Long: "Run the dr-controlplane OCM addon manager on the hub. It reconciles a " +
			"HelmAgentAddon that renders the embedded dr-controlplane-agent chart onto " +
			"every ManagedCluster carrying the addon, replacing the manual per data " +
			"center helm install of the agent.",
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return ocm.RunManagerController()
		},
	}
	return cmd
}
