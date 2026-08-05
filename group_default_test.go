package arbitro

import "testing"

// The group default must be UNCONDITIONAL: group = Group else Name else stream,
// regardless of the delivery mode. The previous form only filled the group in
// when !cfg.Fanout, which left fanout consumers putting an empty group on the
// wire — the exact request the broker now rejects.
func TestResolveConsumerNamingDefaults(t *testing.T) {
	cases := []struct {
		desc      string
		cfgName   string
		cfgGroup  string
		stream    string
		wantName  string
		wantGroup string
	}{
		{
			desc:    "explicit name and group are untouched",
			cfgName: "worker", cfgGroup: "workers", stream: "orders",
			wantName: "worker", wantGroup: "workers",
		},
		{
			desc:    "empty group falls back to the consumer name",
			cfgName: "worker", cfgGroup: "", stream: "orders",
			wantName: "worker", wantGroup: "worker",
		},
		{
			desc:    "empty name falls back to the stream name",
			cfgName: "", cfgGroup: "workers", stream: "orders",
			wantName: "orders", wantGroup: "workers",
		},
		{
			desc:    "empty name and group both fall back to the stream name",
			cfgName: "", cfgGroup: "", stream: "orders",
			wantName: "orders", wantGroup: "orders",
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			name, group := resolveConsumerNaming(tc.cfgName, tc.cfgGroup, tc.stream)
			if name != tc.wantName {
				t.Errorf("name = %q, want %q", name, tc.wantName)
			}
			if group != tc.wantGroup {
				t.Errorf("group = %q, want %q", group, tc.wantGroup)
			}
		})
	}
}

// Regression guard for the `!cfg.Fanout` condition that used to gate the group
// default: fanout consumers must get a group too. The name-derived group is a
// group of one (consumer names are unique per stream), so fanout delivery
// semantics are unchanged.
func TestResolveConsumerNamingNeverReturnsEmptyGroup(t *testing.T) {
	for _, tc := range []struct{ cfgName, cfgGroup, stream string }{
		{"", "", "orders"},
		{"state-w7", "", "orders"},
		{"", "", "_wf_pipeline_state"},
	} {
		name, group := resolveConsumerNaming(tc.cfgName, tc.cfgGroup, tc.stream)
		if group == "" {
			t.Errorf("resolveConsumerNaming(%q, %q, %q) returned an empty group", tc.cfgName, tc.cfgGroup, tc.stream)
		}
		if name == "" {
			t.Errorf("resolveConsumerNaming(%q, %q, %q) returned an empty name", tc.cfgName, tc.cfgGroup, tc.stream)
		}
	}
}
