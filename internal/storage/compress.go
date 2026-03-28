package storage

import (
	"github.com/klauspost/compress/zstd"
)

// Compressor handles compression/decompression.
type Compressor struct {
	encoder   *zstd.Encoder
	decoder   *zstd.Decoder
	threshold int
	level     int
}

// NewCompressor creates a new compressor with the given threshold and compression level.
func NewCompressor(threshold, level int) (*Compressor, error) {
	if threshold <= 0 {
		threshold = 4096
	}
	if level < 0 {
		level = 0
	}
	if level > 9 {
		level = 9
	}

	encoderLevel := zstd.EncoderLevelFromZstd(level)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(encoderLevel))
	if err != nil {
		return nil, err
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		encoder.Close()
		return nil, err
	}

	return &Compressor{
		encoder:   encoder,
		decoder:   decoder,
		threshold: threshold,
		level:     level,
	}, nil
}

// Encode compresses src and returns the compressed slice.
func (c *Compressor) Encode(src []byte) ([]byte, error) {
	return c.encoder.EncodeAll(src, nil), nil
}

// Decode decompresses src and returns the decompressed slice.
func (c *Compressor) Decode(src []byte) ([]byte, error) {
	return c.decoder.DecodeAll(src, nil)
}

// Threshold returns the compression threshold.
func (c *Compressor) Threshold() int {
	return c.threshold
}

// Close closes the compressor.
func (c *Compressor) Close() error {
	if c.encoder != nil {
		c.encoder.Close()
	}
	if c.decoder != nil {
		c.decoder.Close()
	}
	return nil
}
