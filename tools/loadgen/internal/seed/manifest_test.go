package seed

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteManifestRoundTripsAndLeavesNoTemporaryFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "nested", "manifest.json")
	manifest := Manifest{
		CreatedAt:  time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
		Partitions: 12,
		Users:      []User{{ID: "1", Email: "loadgen-1@loadgen.invalid"}},
		Auctions:   []ManifestAuction{{ID: "9", OwnerID: "1", Partition: 3}},
	}

	err := WriteManifest(path, manifest)
	if err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Manifest
	err = json.Unmarshal(raw, &decoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Partitions != 12 || len(decoded.Auctions) != 1 || decoded.Auctions[0].Partition != 3 {
		t.Errorf("manifest did not round trip: %+v", decoded)
	}

	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("directory holds %d entries, want only the manifest", len(entries))
	}
}

func TestWriteManifestReplacesAnExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	err := os.WriteFile(path, []byte("stale"), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	err = WriteManifest(path, Manifest{Partitions: 6})
	if err != nil {
		t.Fatal(err)
	}

	raw, _ := os.ReadFile(path)
	var decoded Manifest
	err = json.Unmarshal(raw, &decoded)
	if err != nil || decoded.Partitions != 6 {
		t.Fatalf("file was not replaced: %v %s", err, raw)
	}
}
