# Installing dr-controlplane on EKS, AKS and GKE

The reference deployment in the README targets on-prem clusters that share a flat
routable network: the etcd members run with `hostNetwork` on node IPs and keep their data
in a `hostPath`. On a managed cloud cluster both of those assumptions are wrong, because a
node's IP and a node's disk are disposable. This guide covers the differences.

The on-prem path is unchanged. Everything here is opt-in, and leaving these values alone
renders exactly what it rendered before.

---

## What is different in cloud, and why

| On-prem reference | Cloud | Reason |
|---|---|---|
| `etcd.member.hostNetwork: true` | `false` + a Service | Node IPs move when a node is replaced (ASG/VMSS/spot). `etcd.peers` is the same string in every cluster, so a moving address breaks the quorum everywhere at once. |
| `etcd.member.hostPath` | `etcd.member.persistence` (PVC) | Node-local data dies with the node. A member that returns with an empty data dir has lost its place in the quorum. |
| etcd over `http://` | `etcd.member.tls.enabled: true` | etcd holds the primary-DC Leases. Anyone who reaches port 2379 decides which site is writable. |
| `controlplane.service.type: ClusterIP` | `LoadBalancer` (internal) | Agents live in other clusters and cannot reach a ClusterIP. |

### Prerequisite the chart cannot do for you

**The clusters must be able to route to each other.** ServiceExport and cloud load
balancers do not create connectivity by themselves.

- **AWS**: VPC peering or a Transit Gateway between the VPCs, with security groups
  allowing 2379/2380 between the node subnets and 9443 to the control plane.
- **Azure**: VNet peering (global peering across regions), NSGs allowing the same ports.
- **GCP**: VPC Network Peering or Shared VPC.

Use **internal** load balancers. These endpoints carry the authority to decide which data
centre is writable and must never be reachable from the internet.

---

## Step 1: DNS names first

Pick a stable DNS name per etcd member and one per control plane endpoint, and create them
before installing. Everything else keys off these, including the certificates.

```
etcd-dc-a.internal.example.com     -> internal LB in cluster A
etcd-dc-b.internal.example.com     -> internal LB in cluster B
etcd-dc-c.internal.example.com     -> internal LB in cluster C
drcp.internal.example.com          -> internal LB in front of the control plane
```

Names rather than IPs matter here: `etcd.peers` is a single string that must be byte
identical in every cluster, and cloud load balancer IPs are not stable unless you pin them.

## Step 2: etcd certificates

Each member needs a certificate whose SANs cover the name its peers dial. Issue them from
one CA shared by all three members.

```bash
# one CA for the etcd cluster
openssl req -x509 -newkey rsa:4096 -nodes -days 3650 \
  -keyout etcd-ca.key -out etcd-ca.crt -subj "/CN=dr-etcd-ca"

# per member, with the SAN that matches its advertiseHost
for dc in a b c; do
  openssl req -newkey rsa:4096 -nodes \
    -keyout etcd-dc-$dc.key -out etcd-dc-$dc.csr -subj "/CN=dc-$dc"
  openssl x509 -req -in etcd-dc-$dc.csr -CA etcd-ca.crt -CAkey etcd-ca.key \
    -CAcreateserial -days 825 -out etcd-dc-$dc.crt \
    -extfile <(printf "subjectAltName=DNS:etcd-dc-$dc.internal.example.com\nextendedKeyUsage=serverAuth,clientAuth")
done
```

`extendedKeyUsage` must include **both** `serverAuth` and `clientAuth`: with
`--peer-client-cert-auth` each member is a client of the others.

Create the Secret in each cluster, in the release namespace:

```bash
kubectl -n dc-failover create secret generic etcd-dc-a-tls \
  --from-file=ca.crt=etcd-ca.crt \
  --from-file=tls.crt=etcd-dc-a.crt \
  --from-file=tls.key=etcd-dc-a.key
```

## Step 3: the apiserver CA

`controlplane.apiserverCASecret` is **required off OpenShift**. The chart can mint it
(`generateApiserverCA: true`) and will reuse an existing Secret on upgrade.

> **If you deploy with ArgoCD, Flux, or any `helm template` pipeline, set
> `generateApiserverCA: false` and create the Secret yourself.**
>
> The reuse logic depends on Helm `lookup`, which returns empty when there is no cluster
> connection. Under `helm template` a **fresh CA is minted on every render**, so a GitOps
> sync would re-sign the apiserver with a CA that no distributed agent kubeconfig trusts,
> and every agent fails with `x509: certificate signed by unknown authority` at once. You
> can see this locally: render the chart twice and compare the `ca.crt` values.

```bash
openssl req -x509 -newkey rsa:4096 -nodes -days 3650 \
  -keyout apiserver-ca.key -out apiserver-ca.crt -subj "/CN=dr-controlplane-ca"
kubectl -n dc-failover create secret generic dr-apiserver-ca \
  --from-file=ca.crt=apiserver-ca.crt --from-file=ca.key=apiserver-ca.key
```

Back this Secret up. Losing it invalidates every agent credential in every data centre.

## Step 4: install

One install per data centre. `etcd.peers` must be identical everywhere.

```yaml
# values-dc-a.yaml
namespace: dc-failover

controlplane:
  enabled: true
  image: quay.io/open-cluster-management/multicluster-controlplane:v0.7.0
  replicas: 1                      # the data-dir PVC is ReadWriteOnce
  externalHostname: drcp.internal.example.com
  apiserverCASecret: dr-apiserver-ca
  generateApiserverCA: false       # created in step 3
  persistence:
    enabled: true
    size: 1Gi
    storageClassName: gp3          # AKS: managed-csi   GKE: standard-rwo
  service:
    type: LoadBalancer

agent:
  enabled: true
  dcName: dc-a
  coordKubeconfigSecret: coord-kubeconfig

etcd:
  deploy: true
  member:
    name: dc-a
    advertiseHost: etcd-dc-a.internal.example.com
    hostNetwork: false
    persistence:
      enabled: true
      size: 8Gi
      storageClassName: gp3
    service:
      enabled: true
      type: LoadBalancer
      annotations:
        # AWS
        service.beta.kubernetes.io/aws-load-balancer-internal: "true"
        service.beta.kubernetes.io/aws-load-balancer-scheme: internal
        # Azure
        # service.beta.kubernetes.io/azure-load-balancer-internal: "true"
    tls:
      enabled: true
      secretName: etcd-dc-a-tls
  peers:
    - dc-a=https://etcd-dc-a.internal.example.com:2380
    - dc-b=https://etcd-dc-b.internal.example.com:2380
    - dc-c=https://etcd-dc-c.internal.example.com:2380
  initialClusterState: new         # "existing" when joining a running quorum
  heartbeatIntervalMs: 500
  electionTimeoutMs: 5000
```

```bash
helm upgrade --install dr-controlplane ./charts/dr-controlplane \
  -n dc-failover --create-namespace -f values-dc-a.yaml
```

Repeat for `dc-b` and `dc-c`, changing only `agent.dcName`, `etcd.member.name`,
`etcd.member.advertiseHost` and `etcd.member.tls.secretName`. Install the control plane in
one data centre only; the others run the agent and their etcd member.

### Bootstrap order

1. Create the LB DNS records (step 1) so `advertiseHost` resolves before etcd starts.
2. Install all three etcd members with `initialClusterState: new` close together. A member
   whose peers do not resolve will restart until they do.
3. Install the control plane once the quorum is formed.
4. Install the agents.

### WAN tuning

`heartbeatIntervalMs: 500` / `electionTimeoutMs: 5000` are the defaults and suit
cross-region round trips. The rule is heartbeat above the p99 RTT and election timeout at
least ten times the heartbeat. Cross-continent links may need more.

Do **not** lower `agent.election.leaseDurationSeconds` below 30. The database data plane
fences itself on a 30 second marker TTL; a shorter lease lets a survivor become writable
before the old primary has fenced, and two sites accept writes at once.

---

## Verify

```bash
# quorum, over TLS
kubectl -n dc-failover exec dr-controlplane-etcd-dc-a -- etcdctl \
  --endpoints=https://etcd-dc-a.internal.example.com:2379 \
  --cacert=/etc/etcd-tls/ca.crt --cert=/etc/etcd-tls/tls.crt --key=/etc/etcd-tls/tls.key \
  endpoint status --cluster -w table

# plaintext must now be refused
kubectl -n dc-failover exec dr-controlplane-etcd-dc-a -- \
  etcdctl --endpoints=http://etcd-dc-a.internal.example.com:2379 endpoint health   # expect failure

# leases
kubectl --kubeconfig coord.kubeconfig -n dc-failover get lease
```

---

## Known limitations

**The etcd member is a bare Pod.** Nothing reschedules it. If its node is lost the member
stays down until Helm runs again. With `persistence.enabled: true` the data survives, so
recovery is a re-apply rather than a restore, but plan for it: pin members to a node group
you control and alert on the Pod disappearing.

**Agent identity is a shared kubeconfig.** The addon manager distributes
`coord-kubeconfig` to every managed cluster inside a ManifestWork, which is an ordinary
API object in the cluster namespace on the hub. Anyone who can read ManifestWorks there can
read the credential. CSR-based per-agent identity is planned and will replace this.

**Two-site topologies.** The supported two-site layout puts the third etcd vote on the DR
site, so losing the DR site loses two of three votes and no failover can occur. Use three
separate failure domains where you can, and set `topology.requireSpread: true` to enforce
it.

**Rotating the apiserver CA** invalidates every distributed kubeconfig at once. The addon
manager redistributes automatically, but the KubeDB operator must be restarted to re-read
the mounted file.
