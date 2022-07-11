package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"path"
)

const bucket = "zenskar"
const accessKeyID = "****"
const accessSecretKey = "****"
const region = "asia-south2"
const endpoint = "storage.googleapis.com"

type s3Client struct {
	session *session.Session
	prefix  string
}

func NewS3Client(prefix string) *s3Client {
	s := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(accessKeyID, accessSecretKey, ""),
		Endpoint:    aws.String(endpoint),
	}))
	return &s3Client{session: s, prefix: prefix}
}

func (s *s3Client) upload(key string, r io.Reader) error {
	uploader := s3manager.NewUploader(s.session, func(u *s3manager.Uploader) {
		u.PartSize = 32 * 1024 * 1024 // 32MB per part
	})
	keyWithPrefix := path.Join("/", s.prefix, key)
	input := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(keyWithPrefix),
		Body:   r,
	}
	_, err := uploader.Upload(input)
	return err
}
