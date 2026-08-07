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

// TestAgentHealthyReflectsCoordinationPlaneReachability pins the fix for the live
// failure where all three agents were unable to reach a rebuilt control plane for ~35
// minutes (every call failing x509) while every ManagedClusterAddOn still reported
// Available=True and no pod ever restarted.
func TestAgentHealthyReflectsCoordinationPlaneReachability(t *testing.T) {
	a := &Agent{}

	// Never renewed: unhealthy. A fresh Agent from New() is seeded, but the zero value
	// must not read as healthy.
	if a.Healthy() {
		t.Fatal("an agent that has never reached the control plane must not be healthy")
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

// TestNewAgentSeedsHealth pins that startup is covered by the same window, so an agent
// is not declared dead before its first renewal has had a chance to run.
func TestNewAgentSeedsHealth(t *testing.T) {
	a := New(Options{}, nil, nil)
	if !a.Healthy() {
		t.Fatal("a freshly constructed agent must start healthy so startup is not a restart loop")
	}
}
