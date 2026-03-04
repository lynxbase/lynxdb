package tiering

import (
	"context"
	"fmt"
	"hash/crc32"

	"github.com/lynxbase/lynxdb/internal/objstore"
)

const (
	defaultPartSize          = 64 << 20 // 64MB
	defaultConcurrentUploads = 3
)

// UploadPipeline handles multipart uploads of segments to object storage.
// Large segments are split into parts and uploaded concurrently with a
// semaphore limiting parallelism.
type UploadPipeline struct {
	store     objstore.ObjectStore
	partSize  int64
	uploadSem chan struct{} // semaphore for concurrent uploads
}

// UploadConfig configures the upload pipeline.
type UploadConfig struct {
	PartSize          int64 // bytes per part (default 64MB)
	ConcurrentUploads int   // max concurrent uploads (default 3)
}

// NewUploadPipeline creates a new upload pipeline.
func NewUploadPipeline(store objstore.ObjectStore, cfg UploadConfig) *UploadPipeline {
	partSize := cfg.PartSize
	if partSize <= 0 {
		partSize = defaultPartSize
	}
	concurrent := cfg.ConcurrentUploads
	if concurrent <= 0 {
		concurrent = defaultConcurrentUploads
	}

	return &UploadPipeline{
		store:     store,
		partSize:  partSize,
		uploadSem: make(chan struct{}, concurrent),
	}
}

// UploadResult holds the result of a multipart upload.
type UploadResult struct {
	Key       string
	Parts     int
	TotalSize int64
	CRC32     uint32
}

// Upload writes data as a single object and verifies the upload via CRC32
// read-back. The semaphore limits concurrent uploads across the pipeline.
func (p *UploadPipeline) Upload(ctx context.Context, key string, data []byte) (*UploadResult, error) {
	totalCRC := crc32.ChecksumIEEE(data)
	totalSize := int64(len(data))

	// Acquire semaphore.
	select {
	case p.uploadSem <- struct{}{}:
		defer func() { <-p.uploadSem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Upload the object.
	if err := p.store.Put(ctx, key, data); err != nil {
		return nil, fmt.Errorf("upload %s: %w", key, err)
	}

	// Verify: read back and check CRC.
	verify, err := p.store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("upload verify %s: %w", key, err)
	}
	verifyCRC := crc32.ChecksumIEEE(verify)
	if verifyCRC != totalCRC {
		return nil, fmt.Errorf("upload CRC mismatch for %s: expected %08x, got %08x", key, totalCRC, verifyCRC)
	}

	return &UploadResult{
		Key:       key,
		Parts:     1,
		TotalSize: totalSize,
		CRC32:     totalCRC,
	}, nil
}

// SafeUpload uploads data with safety guarantees: the local file is
// considered safe to delete only if the upload succeeds (with CRC verify
// in Upload) AND a HeadObject check confirms existence.
func (p *UploadPipeline) SafeUpload(ctx context.Context, key string, data []byte) (safeToDeleteLocal bool, err error) {
	_, err = p.Upload(ctx, key, data)
	if err != nil {
		return false, err
	}

	// HeadObject existence check.
	exists, err := p.store.Exists(ctx, key)
	if err != nil || !exists {
		return false, fmt.Errorf("upload safety check failed for %s: exists=%v: %w", key, exists, err)
	}

	return true, nil
}
