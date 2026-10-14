package seed

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ManifestAuction is one auction that exists, with the Kafka partition its
// events will use so a scenario can choose auctions by partition.
type ManifestAuction struct {
	ID                       string `json:"id"`
	OwnerID                  string `json:"ownerId"`
	CurrencyCode             string `json:"currencyCode"`
	EndsAt                   string `json:"endsAt"`
	StartingPriceMinor       string `json:"startingPriceMinor"`
	MinimumBidIncrementMinor string `json:"minimumBidIncrementMinor"`
	Partition                int    `json:"partition"`
}

// Manifest is what a seeding run leaves behind for the stages that follow it.
type Manifest struct {
	CreatedAt       time.Time         `json:"createdAt"`
	Partitions      int               `json:"partitions"`
	Users           []User            `json:"users"`
	Auctions        []ManifestAuction `json:"auctions"`
	Failed          int               `json:"failed"`
	FailureSamples  []string          `json:"failureSamples,omitempty"`
	PartitionCounts []int             `json:"partitionCounts"`
}

// WriteManifest writes the manifest to a temporary file and renames it, so a
// reader never sees a half-written file.
func WriteManifest(path string, manifest Manifest) error {
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}

	directory := filepath.Dir(path)
	err = os.MkdirAll(directory, 0o755)
	if err != nil {
		return fmt.Errorf("create manifest directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".manifest-*")
	if err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	defer os.Remove(temporary.Name())

	_, err = temporary.Write(append(encoded, '\n'))
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
