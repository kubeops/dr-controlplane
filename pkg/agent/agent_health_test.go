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
	"testing"
	"time"
)

// TestWriterHealthyReflectsCoordinationPlaneReachability pins the fix for the live
// failure where all three agents were unable to reach a rebuilt control plane for ~35
// minutes (every call failing x509) while every ManagedClusterAddOn still reported
// Available=True and no pod ever restarted. The WRITER replica is judged by hub-write
// freshness.
func TestWriterHealthyReflectsCoordinationPlaneReachability(t *testing.T) {
	a := &Agent{}
	a.isWriter.Store(true)

	// Never renewed: unhealthy. A fresh Agent from New() is seeded, but the zero value
	// must not read as healthy.
	if a.Healthy() {
		t.Fatal("a writer that has never reached the control plane must not be healthy")
	}

	a.noteHealthOK()
	if !a.Healthy() {
		t.Fatal("a just-succeeded renewal must be healthy")
	}

	// Just inside the window stays healthy, so a transient blip never restarts a pod.
	a.lastHealthOK.Store(time.Now().Add(-(HealthStaleAfter - 10*time.Second)).UnixNano())
	if !a.Healthy() {
		t.Fatalf("within %s of a success must still be healthy", HealthStaleAfter)
	}

	// Sustained failure past the window is what the liveness probe should catch.
	a.lastHealthOK.Store(time.Now().Add(-(HealthStaleAfter + time.Second)).UnixNano())
	if a.Healthy() {
		t.Fatalf("no successful write for longer than %s must report unhealthy", HealthStaleAfter)
	}
}

// TestNonWriterHealthyReflectsObservation pins the standby side of the role-aware
// probe: a non-writer never writes to the hub BY DESIGN, so judging it by hub-write
// freshness would restart the hot standby forever. Its job is a live view of the hub,
// so it is healthy exactly when its Lease observation has synced.
func TestNonWriterHealthyReflectsObservation(t *testing.T) {
	a := New(Options{}, nil, nil, nil)

	if a.Healthy() {
		t.Fatal("a non-writer whose hub observation has not synced must not be healthy")
	}
	a.observationSynced.Store(true)
	if !a.Healthy() {
		t.Fatal("a non-writer with a synced hub observation is a healthy hot standby")
	}
	// Hub-write staleness must NOT matter for a non-writer.
	a.lastHealthOK.Store(time.Now().Add(-(HealthStaleAfter * 10)).UnixNano())
	if !a.Healthy() {
		t.Fatal("hub-write freshness must not be applied to a replica that is not allowed to write")
	}
}

// TestWriterPromotionReseedsHealthWindow pins the transition hazard: a replica that
// was a standby for hours flips to writer semantics the moment it wins the election,
// and its lastHealthOK clock started long ago. writerRunnable re-seeds the window
// before flipping the role so a brand-new writer is not instantly probe-killed; this
// test pins the invariant the re-seed provides.
func TestWriterPromotionReseedsHealthWindow(t *testing.T) {
	a := New(Options{}, nil, nil, nil)
	a.observationSynced.Store(true)
	// Standby for a long time: hub-write clock is ancient, standby is healthy.
	a.lastHealthOK.Store(time.Now().Add(-time.Hour).UnixNano())
	if !a.Healthy() {
		t.Fatal("long-lived standby must be healthy")
	}
	// The promotion sequence: re-seed FIRST, then flip the role.
	a.noteHealthOK()
	a.isWriter.Store(true)
	if !a.Healthy() {
		t.Fatal("a freshly promoted writer must start inside the health window")
	}
}
