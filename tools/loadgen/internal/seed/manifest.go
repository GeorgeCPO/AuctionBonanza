package seed

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ManifestAuction is one auction that exists, with the Kafka partition its
// events will use so a scenario can choose auctions by partition.
type ManifestAuction struct {
	CreatedAuction
	Partition int `json:"partition"`
}

// Manifest is what a seeding run leaves behind for the stages that follow it.
type Manifest struct {
	CreatedAt      time.Time         `json:"createdAt"`
	Partitions     int               `json:"partitions"`
	Users          []User            `json:"users"`
	Auctions       []ManifestAuction `json:"auctions"`
	Failed         int               `json:"failed"`
	FailureSamples []string          `json:"failureSamples,omitempty"`
}

// PartitionCounts is how many of the auctions map to each partition.
func (m Manifest) PartitionCounts() []int {
	counts := make([]int, m.Partitions)
	for _, auction := range m.Auctions {
		counts[auction.Partition]++
	}
	return counts
}

// WriteManifest streams the manifest to a temporary file and renames it, so a
// reader never sees a half-written file and a manifest of a million auctions is
// never held a second time as one encoded buffer. The encoding is compact; pipe
// it through jq to read it.
func WriteManifest(path string, manifest Manifest) error {
	directory := filepath.Dir(path)
	err := os.MkdirAll(directory, 0o755)
	if err != nil {
		return fmt.Errorf("create manifest directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".manifest-*")
	if err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	defer os.Remove(temporary.Name())

	buffered := bufio.NewWriter(temporary)
	err = json.NewEncoder(buffered).Encode(manifest)
	if err == nil {
		err = buffered.Flush()
	}
	if err != nil {
		temporary.Close()
		return fmt.Errorf("write manifest: %w", err)
	}
	err = temporary.Close()
	if err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return os.Rename(temporary.Name(), path)
}
