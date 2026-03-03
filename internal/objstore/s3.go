package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// S3Store implements ObjectStore using AWS S3.
type S3Store struct {
	client *s3.Client
	bucket string
	prefix string
}

// S3Options configures an S3Store.
type S3Options struct {
	Endpoint       string // custom endpoint for S3-compatible stores (MinIO, etc.)
	ForcePathStyle bool   // use path-style addressing instead of virtual-hosted
}

// NewS3Store creates an S3-backed object store.
func NewS3Store(ctx context.Context, bucket, region, prefix string) (*S3Store, error) {
	return NewS3StoreWithOptions(ctx, bucket, region, prefix, S3Options{})
}

// NewS3StoreWithOptions creates an S3-backed object store with custom options.
func NewS3StoreWithOptions(ctx context.Context, bucket, region, prefix string, opts S3Options) (*S3Store, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("objstore: load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if opts.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		})
	}
	if opts.ForcePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	return &S3Store{
		client: s3.NewFromConfig(cfg, s3Opts...),
		bucket: bucket,
		prefix: prefix,
	}, nil
}

func (s *S3Store) key(k string) string {
	return s.prefix + k
}

func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.key(key)),
		Body:          io.NopCloser(io.NewSectionReader(newBytesReaderAt(data), 0, int64(len(data)))),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return fmt.Errorf("objstore: s3 put %s: %w", key, err)
	}

	return nil
}

func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
	})
	if err != nil {
		return nil, fmt.Errorf("objstore: s3 get %s: %w", key, err)
	}
	defer out.Body.Close()

	return io.ReadAll(out.Body)
}

func (s *S3Store) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	rangeStr := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
		Range:  aws.String(rangeStr),
	})
	if err != nil {
		return nil, fmt.Errorf("objstore: s3 get-range %s: %w", key, err)
	}
	defer out.Body.Close()

	return io.ReadAll(out.Body)
}

func (s *S3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
	})
	if err != nil {
		return fmt.Errorf("objstore: s3 delete %s: %w", key, err)
	}

	return nil
}

func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := s.key(prefix)
	var keys []string

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("objstore: s3 list %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			// Strip the store prefix to return relative keys.
			k := *obj.Key
			if len(k) > len(s.prefix) {
				k = k[len(s.prefix):]
			}
			keys = append(keys, k)
		}
	}

	return keys, nil
}

func (s *S3Store) Copy(ctx context.Context, srcKey, dstKey string) error {
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		CopySource: aws.String(s.bucket + "/" + s.key(srcKey)),
		Key:        aws.String(s.key(dstKey)),
	})
	if err != nil {
		return fmt.Errorf("objstore: s3 copy %s -> %s: %w", srcKey, dstKey, err)
	}

	return nil
}

func (s *S3Store) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
	})
	if err != nil {
		// If the error indicates NotFound, return false without error.
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotFound" {
			return false, nil
		}

		return false, fmt.Errorf("objstore: s3 exists %s: %w", key, err)
	}

	return true, nil
}
