package storage

import (
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/percona/pitr_restore/pkg/binlog"
)

type S3Storage struct {
	Endpoint        string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	Prefix          string
	svc             *s3.S3
}

func NewS3Storage(endpoint, bucket, accessKeyId, secretAccessKey, prefix string) BinlogStorage {
	session := session.Must(session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(accessKeyId, secretAccessKey, ""),
	}))
	svc := s3.New(session)

	return &S3Storage{
		Endpoint:        endpoint,
		Bucket:          bucket,
		AccessKeyID:     accessKeyId,
		SecretAccessKey: secretAccessKey,
		Prefix:          prefix,
		svc:             svc,
	}
}

// ListBinlogs returns a list of binlogs available in the storage
func (s *S3Storage) ListBinlogs() ([]binlog.Binlog, error) {
	out, err := s.svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(s.Bucket),
		Prefix: aws.String(s.Prefix),
	})
	if err != nil {
		return nil, err
	}

	binlogs := make([]binlog.Binlog, 0)

	log.Printf("Found %d binlogs", len(out.Contents))

	for _, obj := range out.Contents {
		if strings.HasSuffix(*obj.Key, "-gtid-set") {
			continue
		}

		log.Printf("Found binlog %s, size: %d", *obj.Key, *obj.Size)

		binlogs = append(binlogs, binlog.Binlog{
			Name: *obj.Key,
			Size: *obj.Size,
		})
	}

	return binlogs, nil
}

func (s *S3Storage) DownloadBinlog(blog binlog.Binlog) ([]byte, error) {
	buf := aws.NewWriteAtBuffer([]byte{})

	downloader := s3manager.NewDownloaderWithClient(s.svc)
	n, err := downloader.Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(blog.Name),
	})

	log.Printf("Downloaded %d bytes from %s", n, blog.Name)

	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
