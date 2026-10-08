# AGENTS.md

This file provides guidance to coding agents (e.g. Claude Code, claude.ai/code) when working with code in this repository.

## Repository purpose

Go module `kubeops.dev/dr-controlplane` produces a single binary, `dr-controlplane`, the common DC failover service. It runs a three data center etcd quorum and an OCM control plane that serves the standard `coordination.k8s.io` Lease API as the cross data center failover signal. Other applications, starting with the KubeDB DC/DR driver, read that Lease to decide which data center is primary.

The split brain guarantee is the etcd majority: moving a Lease is an etcd write that needs a majority, so a partitioned minority cannot renew and self fences. The service never promotes or demotes a workload itself; it only publishes which data center the quorum trusts.

## The binary, one command with subcommands

```
dr-controlplane agent       per data center DC agent (one per DC): renews the DC health Lease, contends for the primary DC Lease for Member scopes, runs the coordinated failback handoff
dr-controlplane controller  topology controller (one): reads PlacementPolicies, ensures one primary DC Lease per scope
dr-controlplane status      CLI: scopes, members, current primary, per DC health
dr-controlplane switchover  CLI: request a planned coordinated handoff
dr-controlplane version
```

## Architecture

- `cmd/dr-controlplane/main.go` wires `pkg/cmds.NewRootCmd()`, initializes klog flags, runs with a signal context.
- `cmd/dr-controlplane/version.go` holds the ldflag version variables (`gomodules.xyz/x/version`).
- `pkg/cmds/` Cobra tree: `root.go`, `agent.go`, `controller.go`, `status.go`, `switchover.go`, `util.go` (kubeconfig and lease helpers).
- `pkg/leases/` the naming and annotation contract shared by every component: `primary-dc`, `primary-dc-<group>`, `dc-health-<dc>`, the `member-dcs` and `handoff-to` annotations, the `Scope` type. Treat this as the wire protocol between the controller, the agents, and consumers.
- `pkg/agent/` the DC agent: `health.go` (renew the DC health Lease), `election.go` (per scope `client-go` leader election, started or paused per desired state), `handoff.go` (the `desiredContend` decision and handoff annotation clearing), `agent.go` (informer driven orchestration), `metrics.go`, `options.go`.
- `pkg/topology/` `topology.go` (pure derivation of Member/Arbiter/Witness sets from PlacementPolicies, unit tested) and `controller.go` (dynamic informer on PlacementPolicy, ensures the Leases).
- `pkg/client/` the consumer library: a Lease informer that exposes the current primary DC per scope and fires callbacks on change.
- The PlacementPolicy extension (`FailoverPolicy`, `DistributionRule.Role`) lives in `kubeops.dev/petset/apis/apps/v1`, consumed here through a local replace; it is not defined in this repo.
- `charts/dr-controlplane/` Helm chart: etcd quorum, the config only control plane (`controlplane server --controlplane-config-dir` with an external etcd `ocmconfig.yaml`), the agent (one install per DC), the controller.
- `Dockerfile.in` (PROD distroless), `Dockerfile.dbg` (debian + dlv), `Dockerfile.ubi` (Red Hat), `hack/`, `Makefile` are the AppsCode build harness. `vendor/` is checked in.

## Common commands

All Make targets run inside `ghcr.io/appscode/golang-dev`, so Docker must be running.

- `make ci` the CI pipeline: `verify check-license lint build unit-tests`.
- `make build` / `make all-build` build the host or all platform binaries.
- `make fmt`, `make lint`, `make test` (alias for `unit-tests`).
- `make verify` runs `verify-gen verify-modules`; `go mod tidy && go mod vendor` must leave the tree clean.
- `make container` builds PROD, DBG, and UBI images; `make push`, `make docker-manifest`, `make release` publish.
  `make release` requires `APPSCODE_ENV=prod` and a git tag; `make qa` is the untagged, non prod equivalent.
- `make install` / `make uninstall` Helm lifecycle into namespace `dc-failover`.
- `make add-license` / `make check-license` manage the `ltag` headers.
- `make run` runs `go run ./cmd/dr-controlplane agent --dc-name=dc-a` against `$KUBECONFIG` for quick local iteration.

GitHub Actions live in `.github/workflows/`: `ci.yml` runs `make ci` on every PR, `release.yml` runs
`make release` on a tag push, and `release-tracker.yml` reports a merged release PR back to the
`Release-tracker:` PR named in the commit body. The tracker is dormant here: this repo is not part of
an automated release train, so no commit carries that trailer and the job exits after its detect step.

Run a single test with a local Go toolchain:

```
go test ./pkg/topology/... -run TestDeriveTwoDCWithArbiter -v
```

## Conventions

- Module path is `kubeops.dev/dr-controlplane`; keep imports on it. The `dr.open-cluster-management.io/*` Lease annotation keys in `pkg/leases` are unrelated to the module path and must not be renamed with it.
- License is **Apache 2.0** (`LICENSE`). New files need the standard "Copyright AppsCode Inc. and Contributors" header; `hack/license/` holds the templates and `make add-license` stamps them.
- Logging is `k8s.io/klog/v2` everywhere. Use `klog.InfoS` / `klog.ErrorS` structured logging, not `fmt` or other loggers.
- The vendor directory is checked in; `verify-modules` fails if `go mod tidy && go mod vendor` is not clean.
- `kubeops.dev/petset` is a local replace (`../../kubeops.dev/petset`), so keep the petset checkout beside this repo in the GOPATH layout. Editing the PlacementPolicy API means editing petset's `apis/apps/v1` and its `zz_generated.deepcopy.go`.
- The control plane apiserver is **never forked**. It is configured through `ocmconfig.yaml` (external etcd mode) and run from its published image. Changes to the control plane belong in the chart, not in Go.
- `Arbiter` and `Witness` data centers never hold the primary DC Lease. Only `Member` data centers contend. Enforce this anywhere contention is decided (`pkg/agent`).
- The primary DC Lease is a DC ownership signal, not a per database data currency signal. Consumers apply their own lag guard. Do not add promotion logic to this service.
- The Lease names and annotations in `pkg/leases` are a cross component contract (controller writes, agents and consumers read). Changing them is a breaking change.
- Three Dockerfiles, one binary: keep `Dockerfile.in`, `Dockerfile.dbg`, `Dockerfile.ubi` in sync.
- No em-dashes in any file (commas, periods, colons, parentheses).

## Further docs

`DESIGN.md` (architecture and failure model), `TEST.md` (how to exercise each feature), `DEV.md` (development workflow), `README.md` (user guide).
