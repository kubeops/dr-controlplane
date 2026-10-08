/*
Copyright AppsCode Inc. and Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package leases

import "testing"

func TestPrimaryLeaseNames(t *testing.T) {
	if got := GlobalScope.PrimaryLeaseName(); got != "primary-dc" {
		t.Fatalf("global lease name = %q, want primary-dc", got)
	}
	if got := GroupScope("orders").PrimaryLeaseName(); got != "primary-dc-orders" {
		t.Fatalf("group lease name = %q, want primary-dc-orders", got)
	}
	if got := HealthLeaseName("dc-a"); got != "dc-health-dc-a" {
		t.Fatalf("health lease name = %q, want dc-health-dc-a", got)
	}
}

func TestScopeRoundTrip(t *testing.T) {
	for _, s := range []Scope{GlobalScope, GroupScope("orders")} {
		got, ok := ScopeFromPrimaryLeaseName(s.PrimaryLeaseName())
		if !ok || got != s {
			t.Fatalf("round trip of %v failed: got %v ok=%v", s, got, ok)
		}
	}
	if IsPrimaryLeaseName(HealthLeaseName("dc-a")) {
		t.Fatalf("a health lease must not be classified as a primary lease")
	}
}

func TestMemberDCs(t *testing.T) {
	v := FormatMemberDCs([]string{"dc-b", "dc-a", "dc-a", " "})
	if v != "dc-a,dc-b" {
		t.Fatalf("FormatMemberDCs = %q, want dc-a,dc-b (sorted, deduped)", v)
	}
	if !ContainsMember(v, "dc-a") || ContainsMember(v, "dc-c") {
		t.Fatalf("ContainsMember wrong for %q", v)
	}
}
