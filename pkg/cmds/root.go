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

// Package cmds wires the single dr-controlplane binary's subcommands.
package cmds

import (
	"github.com/spf13/cobra"
	v "gomodules.xyz/x/version"
)

// NewRootCmd builds the dr-controlplane root command.
//
// Server subcommands:
//
//	agent       run the per data center DC agent
//	controller  run the topology controller
//
// Operator subcommands:
//
//	status      show scopes, primaries, and per DC health
//	switchover  request a planned coordinated handoff
func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:               "dr-controlplane [command]",
		Short:             "Common DC failover service: a Kubernetes Lease API for cross data center failover",
		DisableAutoGenTag: true,
	}

	rootCmd.AddCommand(v.NewCmdVersion())
	rootCmd.AddCommand(newCmdAgent())
	rootCmd.AddCommand(newCmdController())
	rootCmd.AddCommand(newCmdStatus())
	rootCmd.AddCommand(newCmdSwitchover())

	return rootCmd
}
