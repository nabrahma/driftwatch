package codec_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/codec"
	"github.com/nabrahma/driftwatch/pkg/event"
)

type stubCodec struct{ name string }

func (s stubCodec) Name() string                            { return s.name }
func (stubCodec) Decode([]byte, string, *event.Event) error { return nil }

func TestRegistry_NamesIncludesTheBuiltInCodecs(t *testing.T) {
	assert.Contains(t, codec.Names(), "json")
}

func TestRegistry_NewReturnsTheRegisteredCodec(t *testing.T) {
	c, err := codec.New("json", nil)

	require.NoError(t, err)
	assert.Equal(t, "json", c.Name())
}

func TestRegistry_NewReportsAnUnknownCodecWithTheAvailableNames(t *testing.T) {
	_, err := codec.New("protobuf", nil)

	require.ErrorIs(t, err, codec.ErrUnknownCodec)
	assert.Contains(t, err.Error(), "json", "the error must say what is available")
}

func TestRegistry_NewPropagatesConstructorErrors(t *testing.T) {
	_, err := codec.New("json", map[string]string{"maxDepth": "-1"})

	assert.ErrorIs(t, err, codec.ErrBadConfig)
}

func TestRegistry_RegisterRejectsDuplicateAndEmptyNames(t *testing.T) {
	codec.Register("codec-test-stub", func(map[string]string) (codec.Codec, error) {
		return stubCodec{name: "codec-test-stub"}, nil
	})

	assert.Contains(t, codec.Names(), "codec-test-stub")

	// A silently shadowed codec would decode events with the wrong field
	// mapping and produce divergence findings that are driftwatch's own fault,
	// so a duplicate registration is a programming error worth panicking on.
	assert.Panics(t, func() {
		codec.Register("codec-test-stub", func(map[string]string) (codec.Codec, error) {
			return stubCodec{}, nil
		})
	})

	assert.Panics(t, func() {
		codec.Register("", func(map[string]string) (codec.Codec, error) { return stubCodec{}, nil })
	})

	assert.Panics(t, func() { codec.Register("codec-test-nil", nil) })
}

func TestRegistry_NamesAreSorted(t *testing.T) {
	names := codec.Names()

	for i := 1; i < len(names); i++ {
		assert.LessOrEqual(t, names[i-1], names[i], "Names must be sorted")
	}
}
