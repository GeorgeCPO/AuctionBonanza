package murmur2

import (
	"strconv"
	"testing"
)

// Vectors from Kafka's UtilsTest.testMurmur2.
func TestHashMatchesKafkaVectors(t *testing.T) {
	cases := []struct {
		input string
		want  int32
	}{
		{"21", -973932308},
		{"foobar", -790332482},
		{"a-little-bit-long-string", -985981536},
		{"a-little-bit-longer-string", -1486304829},
		{"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971},
		{"abc", 479470107},
	}

	for _, tc := range cases {
		got := Hash([]byte(tc.input))
		if got != tc.want {
			t.Errorf("Hash(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestPartitionForKeyStaysInRange(t *testing.T) {
	const partitions = 12
	seen := make(map[int]int)

	for id := 1; id <= 12000; id++ {
		partition := PartitionForKey(strconv.Itoa(id), partitions)
		if partition < 0 || partition >= partitions {
			t.Fatalf("partition %d out of range for id %d", partition, id)
		}
		seen[partition]++
	}

	if len(seen) != partitions {
		t.Fatalf("sequential ids reached %d of %d partitions", len(seen), partitions)
	}
	for partition, count := range seen {
		if count < 800 || count > 1200 {
			t.Errorf("partition %d got %d of 12000 sequential ids, expected roughly 1000", partition, count)
		}
	}
}
