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

package agent

import (
	"os"
	"testing"
)

func files(m map[string]string) func(string) ([]byte, error) {
	return func(p string) ([]byte, error) {
		if v, ok := m[p]; ok {
			return []byte(v), nil
		}
		return nil, os.ErrNotExist
	}
}

func TestMemoryLimitFromCgroup(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files map[string]string
		want  int64
		ok    bool
	}{
		{"v2 limit", map[string]string{"/sys/fs/cgroup/memory.max": "268435456\n"}, 268435456, true},
		{"v2 unlimited", map[string]string{"/sys/fs/cgroup/memory.max": "max\n"}, 0, false},
		{"v1 limit", map[string]string{"/sys/fs/cgroup/memory/memory.limit_in_bytes": "134217728\n"}, 134217728, true},
		{"v1 unlimited", map[string]string{"/sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n"}, 0, false},
		{"garbage", map[string]string{"/sys/fs/cgroup/memory.max": "lots"}, 0, false},
		{"no cgroup", map[string]string{}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := memoryLimitFromCgroup(files(tc.files))
			if got != tc.want || ok != tc.ok {
				t.Fatalf("got (%d, %v), want (%d, %v)", got, ok, tc.want, tc.ok)
			}
		})
	}
	if got := softMemoryLimit(1000); got != 900 {
		t.Fatalf("soft limit = %d, want 90%% (900)", got)
	}
}

func TestOptionsWriterIdentityAndWindow(t *testing.T) {
	t.Setenv("POD_NAME", "dr-controlplane-agent-dc-a-7d9f-x2")
	o := DefaultOptions()
	o.DCName = "dc-a"
	if o.WriterIdentity != "dr-controlplane-agent-dc-a-7d9f-x2" {
		t.Fatalf("writer identity %q, want the pod name", o.WriterIdentity)
	}
	if o.HolderPriorityWindow <= 0 {
		t.Fatal("holder priority must be on by default")
	}
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := o
	bad.WriterIdentity = ""
	if bad.Validate() == nil {
		t.Fatal("an empty writer identity must be rejected")
	}
	bad = o
	bad.HolderPriorityWindow = -1
	if bad.Validate() == nil {
		t.Fatal("a negative window must be rejected")
	}
}
