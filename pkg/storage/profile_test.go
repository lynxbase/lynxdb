package storage

import "testing"

func TestResolveProfile(t *testing.T) {
	tests := []struct {
		name     string
		dataDir  string
		s3Bucket string
		want     Profile
		wantStr  string
	}{
		{
			name:    "empty config is ephemeral",
			want:    Ephemeral,
			wantStr: "ephemeral",
		},
		{
			name:    "dataDir only is persistent",
			dataDir: "/data/lynxdb",
			want:    Persistent,
			wantStr: "persistent",
		},
		{
			name:     "dataDir + S3 is tiered",
			dataDir:  "/data/lynxdb",
			s3Bucket: "my-bucket",
			want:     Tiered,
			wantStr:  "tiered",
		},
		{
			name:     "S3 without dataDir is ephemeral",
			s3Bucket: "my-bucket",
			want:     Ephemeral,
			wantStr:  "ephemeral",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveProfile(tt.dataDir, tt.s3Bucket)
			if got != tt.want {
				t.Errorf("ResolveProfile: got %v, want %v", got, tt.want)
			}
			if got.String() != tt.wantStr {
				t.Errorf("String: got %q, want %q", got.String(), tt.wantStr)
			}
		})
	}
}

func TestFeatures_Ephemeral(t *testing.T) {
	f := Features(Ephemeral)
	if f.PartWriter {
		t.Error("ephemeral should not have PartWriter")
	}
	if f.Compaction {
		t.Error("ephemeral should not have Compaction")
	}
	if f.Tiering {
		t.Error("ephemeral should not have Tiering")
	}
	if !f.Cache {
		t.Error("ephemeral should have Cache")
	}
}

func TestFeatures_Persistent(t *testing.T) {
	f := Features(Persistent)
	if !f.PartWriter {
		t.Error("persistent should have PartWriter")
	}
	if !f.Compaction {
		t.Error("persistent should have Compaction")
	}
	if f.Tiering {
		t.Error("persistent should not have Tiering")
	}
	if !f.Cache {
		t.Error("persistent should have Cache")
	}
}

func TestFeatures_Tiered(t *testing.T) {
	f := Features(Tiered)
	if !f.PartWriter {
		t.Error("tiered should have PartWriter")
	}
	if !f.Compaction {
		t.Error("tiered should have Compaction")
	}
	if !f.Tiering {
		t.Error("tiered should have Tiering")
	}
	if !f.Cache {
		t.Error("tiered should have Cache")
	}
}
