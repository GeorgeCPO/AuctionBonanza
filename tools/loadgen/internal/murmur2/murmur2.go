// Package murmur2 maps record keys to partitions the way Kafka's default
// partitioner and librdkafka's murmur2_random do, which is how the outbox
// publisher places events. The load tool uses it to predict which partition an
// auction's events land on.
package murmur2

const (
	seed = uint32(0x9747b28c)
	m    = uint32(0x5bd1e995)
	r    = 24
)

// Hash is Kafka's Utils.murmur2.
func Hash(data []byte) int32 {
	length := len(data)
	h := seed ^ uint32(length)

	blocks := length / 4
	for i := 0; i < blocks; i++ {
		offset := i * 4
		k := uint32(data[offset]) |
			uint32(data[offset+1])<<8 |
			uint32(data[offset+2])<<16 |
			uint32(data[offset+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}

	tail := length &^ 3
	switch length % 4 {
	case 3:
		h ^= uint32(data[tail+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[tail+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[tail])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return int32(h)
}

// PartitionForKey returns the partition a keyed record goes to on a topic with
// the given number of partitions.
func PartitionForKey(key string, partitions int) int {
	positive := uint32(Hash([]byte(key))) & 0x7fffffff
	return int(positive % uint32(partitions))
}
