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

package agent

import (
	"errors"
	"testing"
	"time"
)

// TestWriterRestartsOnlyForCredentialFailures pins the liveness contract: the probe
// answers "could a restart help", not "is the hub up". It preserves the fix for the
// live x509 incident (a control-plane rebuild minted a new CA, all agents failed
// every call for ~35 minutes, nothing restarted, nothing reloaded the credential)
// while fixing the opposite live incident (2026-08-07: failing the probe on plain
// unreachability bounced the writer role between replicas for a whole hub outage
// and delayed marker recovery past the outage itself).
func TestWriterRestartsOnlyForCredentialFailures(t *testing.T) {
	a := &Agent{}
	a.isWriter.Store(true)

	// Fresh hub writes: healthy regardless of anything else.
	a.noteHealthOK()
	if !a.Healthy() {
		t.Fatal("a writer with fresh hub writes must be healthy")
	}

	// Stale writes with NO credential failures = the hub is unreachable. A restart
	// cannot help; the fence protects the databases; stay alive and keep retrying.
	a.lastHealthOK.Store(time.Now().Add(-(HealthStaleAfter + time.Minute)).UnixNano())
	if !a.Healthy() {
		t.Fatal("plain hub unreachability must not restart the writer")
	}

	// Stale writes AND recent credential-class failures: a restart re-reads the
	// mounted kubeconfig, so fail the probe.
	a.noteAuthFailure()
	if a.Healthy() {
		t.Fatal("stale writes with credential-class failures must fail the probe")
	}

	// Old credential failures no longer matter once they stop occurring.
	a.lastAuthFailure.Store(time.Now().Add(-(HealthStaleAfter + time.Minute)).UnixNano())
	if !a.Healthy() {
		t.Fatal("stale credential failures older than the window must not keep failing the probe")
	}
}

// TestNonWriterAlwaysHealthy pins the standby contract: a non-writer never writes to
// the hub by design, so no hub condition may restart it. Rotten credentials surface
// on its first promotion to writer, which is when they start mattering.
func TestNonWriterAlwaysHealthy(t *testing.T) {
	a := New(Options{}, nil, nil, nil)
	if !a.Healthy() {
		t.Fatal("a standby must be healthy while the process runs")
	}
	a.lastHealthOK.Store(time.Now().Add(-time.Hour).UnixNano())
	a.noteAuthFailure()
	if !a.Healthy() {
		t.Fatal("no hub condition may restart a replica that is not allowed to write")
	}
}

// TestWriterPromotionReseedsHealthWindow pins the transition hazard: a replica that
// was a standby for hours flips to writer semantics the moment it wins the election.
// writerRunnable re-seeds the window before flipping the role so a brand-new writer
// starts inside it even if credential failures follow immediately.
func TestWriterPromotionReseedsHealthWindow(t *testing.T) {
	a := New(Options{}, nil, nil, nil)
	a.lastHealthOK.Store(time.Now().Add(-time.Hour).UnixNano())
	a.noteAuthFailure()

	// The promotion sequence: re-seed FIRST, then flip the role.
	a.noteHealthOK()
	a.isWriter.Store(true)
	if !a.Healthy() {
		t.Fatal("a freshly promoted writer must start inside the health window")
	}
}

// TestCredentialClassError pins the classifier's split: credential problems restart,
// unreachability does not.
func TestCredentialClassError(t *testing.T) {
	for _, err := range []error{
		errors.New("x509: certificate signed by unknown authority"),
		errors.New("Unauthorized"),
		errors.New("invalid bearer token"),
	} {
		if !credentialClassError(err) {
			t.Fatalf("%v must classify as credential-class", err)
		}
	}
	for _, err := range []error{
		nil,
		errors.New("dial tcp 10.2.0.239:9443: connect: connection refused"),
		errors.New("dial tcp 10.2.0.239:9443: i/o timeout"),
		errors.New(`Operation cannot be fulfilled on leases.coordination.k8s.io "primary-dc": the object has been modified`),
	} {
		if credentialClassError(err) {
			t.Fatalf("%v must NOT classify as credential-class", err)
		}
	}
}
