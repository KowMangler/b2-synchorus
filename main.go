package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	s3id := os.Getenv("S3ID")
	if s3id == "" {
		panic("no s3 id")
	}
	s3key := os.Getenv("S3KEY")
	if s3key == "" {
		panic("no s3 key")
	}
	bucket := aws.String("myBucketName")
	key := aws.String("testfile.txt")

	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(s3id, s3key, ""),
		Endpoint:         aws.String("https://s3.us-west-002.backblazeb2.com"),
		Region:           aws.String("us-west-002"),
		S3ForcePathStyle: aws.Bool(true),
	}
	newSession := session.New(s3Config)

	s3Client := s3.New(newSession)

	cparams := &s3.CreateBucketInput{
		Bucket: bucket, // Required
	}
	_, err := s3Client.CreateBucket(cparams)
	if err != nil {
		// Print if any error.
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("Successfully created bucket %s\n", *bucket)

	// Upload a new object "testfile.txt" with the string "S3 Compatible API"
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader("S3 Compatible API"),
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		fmt.Printf("Failed to upload object %s/%s, %s\n", *bucket, *key, err.Error())
		return
	}
	fmt.Printf("Successfully uploaded key %s\n", *key)

	//Get Object
	_, err = s3Client.GetObject(&s3.GetObjectInput{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		fmt.Println("Failed to download file", err)
		return
	}
	fmt.Printf("Successfully Downloaded key %s\n", *key)
}
