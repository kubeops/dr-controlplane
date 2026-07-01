# Development guide

What you need to know to work on this repo. For the big picture read `DESIGN.md`; for the coding-agent summary read `AGENTS.md`.

## Prerequisites

- Go 1.24+ for a local toolchain (`go build`, `go test`, `go vet`).
- Docker, because the `make` targets run inside `ghcr.io/appscode/golang-dev` (which carries `goimports`, `golangci-lint`, `ltag`, `shfmt`, and the cross compilers).
- `helm` and `kind` for chart work and local clusters.
- A checkout of `kubeops.dev/petset` next to this repo. The module path layout is `~/go/src/open-cluster-management.io/dr-controlplane` and `~/go/src/kubeops.dev/petset`, so the local replace `../../kubeops.dev/petset` resolves.

## Repository layout

```
cmd/dr-controlplane        main.go + version.go: entrypoint and ldflag version vars
pkg/cmds                   cobra root + subcommands (agent, controller, status, switchover)
pkg/leases                 lease naming + annotation contract, the Scope type
pkg/agent                  health lease, leader election, coordinated handoff, active DC marker projector, metrics
pkg/topology               derive topology from PlacementPolicy, ensure the Leases
pkg/client                 consumer library (read the failover signal)
charts/dr-controlplane     Helm chart (etcd, control plane, agent, controller)
config/samples             example PlacementPolicies
hack, Makefile, Dockerfile.*  AppsCode build harness
vendor                     checked in dependencies
```

## The petset dependency

The PlacementPolicy extension that marks a workload as cross DC (`FailoverPolicy`, `FailoverTrigger`, and `DistributionRule.Role`) lives in `kubeops.dev/petset/apis/apps/v1`, not in this repo. It is pulled in through a local replace:

```
replace kubeops.dev/petset => ../../kubeops.dev/petset
```

To change that API you edit the petset repo: the types in `apis/apps/v1/placementpolicy_types.go`, the `Validate()` method, and the deepcopy in `apis/apps/v1/zz_generated.deepcopy.go` (regenerated with petset's own `make gen`). After editing petset, run `go mod tidy && go mod vendor` here so the vendored copy updates.

There is also a second replace that pins `sigs.k8s.io/controller-runtime` to the same fork petset uses, so the imported petset apis package resolves one version of it. Keep it.

## Building and verifying

The standard path runs in the build image:

```
make build      # build the binary for the host platform
make fmt        # goimports, gofmt -s, shfmt
make lint       # golangci-lint
make test       # unit tests
make ci         # verify, check-license, lint, build, unit-tests (what CI runs)
```

For fast local iteration without Docker, use the Go toolchain directly:

```
go build ./...
go vet ./...
go test ./pkg/...
go run ./cmd/dr-controlplane agent --dc-name=dc-a --kubeconfig=$HOME/.kube/config   # or: make run
```

The vendor directory is checked in. `make verify` (specifically `verify-modules`) fails the build if `go mod tidy && go mod vendor` would change anything, so run both after touching dependencies and commit the result.

License headers are managed by `ltag` against `hack/license/`. Add the AppsCode Free Trial header to new files with `make add-license`; `make check-license` (part of `make ci`) enforces it.

## Adding a subcommand

Subcommands live in `pkg/cmds`, one file each, each exposing a `newCmdXxx() *cobra.Command`, wired in `root.go`. Server subcommands (`agent`, `controller`) take their context from `cmd.Context()` (a signal context set up in `main.go`) and block until it is cancelled. CLI subcommands (`status`, `switchover`) build a clientset with the shared helpers in `util.go` and return. Follow the existing files: define flags on `cmd.Flags()`, keep the run logic in the relevant `pkg/` package, and let the command file stay thin.

## Where the logic lives

- The leader election, the start/pause of contention, and the handoff decision are in `pkg/agent` (`election.go`, `handoff.go`, `agent.go`). The single most important function is `desiredContend`; it encodes the Member/Arbiter invariant and the coordinated handoff. It is pure and unit tested, so change it there and extend `handoff_test.go`.
- The active DC marker projection is in `pkg/agent/projector.go`: it writes the `activeDC`/`renewTime`/`quiesce` ConfigMap onto the local spoke, with flags in `options.go` (`--spoke-kubeconfig`, `--marker-namespace`, `--marker-refresh-interval`). The marker `renewTime` tracks the primary Lease renewTime so a consumer fence fails closed; keep the consumer fence TTL strictly inside `LeaseDuration`.
- The mapping from PlacementPolicy to scopes and member sets is in `pkg/topology/topology.go` (pure, tested) and the informer plumbing is in `controller.go`.
- The Lease names and annotations every component agrees on are in `pkg/leases`. Treat them as a wire protocol: the controller writes them, the agents and the client library read them, so a rename is a breaking change across all three.

## Conventions

- Logging is `k8s.io/klog/v2` everywhere (`klog.InfoS`, `klog.ErrorS`). Do not introduce another logger.
- The control plane apiserver is configured, never forked. Behavior changes to the control plane go in the chart (`ocmconfig.yaml`), not in Go.
- Keep the service engine agnostic. The Lease is a DC ownership signal; promotion and lag checks belong to the consumer, not here.
- `Arbiter` data centers must never hold a primary Lease (there is no Witness role). Any new contention path has to preserve that.
- Keep `Dockerfile.in`, `Dockerfile.dbg`, and `Dockerfile.ubi` in sync.
- No em-dashes anywhere (commas, periods, colons, parentheses).

## Versioning

Build metadata is injected with ldflags in `hack/build.sh` into the variables in `version.go`, surfaced by `dr-controlplane version`. The version string comes from git tags and branch via the `Makefile`.
