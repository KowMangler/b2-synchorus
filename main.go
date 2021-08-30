package main

import (
	"context"
	"crypto/sha1"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kurin/blazer/b2"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Content struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

var (
	localfiles []Content
)

type FileData struct {
	Name    string
	Sha1Sum string
}

func main() {
	id := os.Getenv("B2ID")
	key := os.Getenv("B2SECRET")
	dat, err := os.ReadFile("testfile.txt")

	fmt.Printf("%+v\n", sha1Sum(dat))
	ctx := context.Background()
	// b2_authorize_account
	b2, err := b2.NewClient(ctx, id, key)
	if err != nil {
		log.Fatalln(err)
	}

	buckets, err := b2.ListBuckets(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(buckets[0].Name())

}

func connectTestB2() {
	id := os.Getenv("B2ID")
	key := os.Getenv("B2SECRET")
	dat, err := os.ReadFile("testfile.txt")

	fmt.Printf("%+v\n", sha1Sum(dat))
	ctx := context.Background()
	// b2_authorize_account
	b2, err := b2.NewClient(ctx, id, key)
	if err != nil {
		log.Fatalln(err)
	}

	buckets, err := b2.ListBuckets(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(buckets[0].Name())
}

func sha1Sum(data []byte) (sum string) {
	sum = fmt.Sprintf("%x", sha1.Sum(data))
	return
}

func dedupeFiles(path string, autoclean bool) (removedCount int, duplicateFiles []FileData, err error) {
	var files []FileData
	start := time.Now()
	err = filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		var returnData FileData
		returnData.Name = path
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Printf("Unable to read file %s\n", path)
			return nil
		}
		fmt.Printf("processing file: %s\n", path)
		returnData.Sha1Sum = sha1Sum(data)
		files = append(files, returnData)
		return nil
	})
	fmt.Printf("parsed %d files in %s\n", len(files), time.Since(start).String())
	if err != nil {
		return
	}

	type delta struct {
		Index   int
		Fileinf FileData
	}
	var deltacheck []delta
	rangeMax := len(files)
	start = time.Now()
	for i := range files {
		// if there are values in deltacheck ensure that i is not one of them,
		// if the index is already parsed and in the slice of the delta struct
		// then there is no reason to process the file since it has already been processed
		// in a previous run. This method is faster than simple checks after the file is in place
		// using other methods (parsing full list) result in time to complete 1213ms, this method is ~230ms for the same 56GB (8371 files)
		var deltaout []delta
		if len(deltacheck) != 0 {
			for c := range deltacheck {
				if deltacheck[c].Index == i {
					goto end
				}
			}
		}

		deltaout = append(deltaout, delta{Index: i, Fileinf: files[i]})
		for s := i + 1; s < rangeMax; s++ {
			if files[i].Sha1Sum == files[s].Sha1Sum {
				deltaout = append(deltaout, delta{Index: s, Fileinf: files[s]})
			}
		}
		if len(deltaout) > 1 {
			deltacheck = append(deltacheck, deltaout...)
		}
	end:
	}
	fmt.Printf("deltacheck content:\n%+v\n", deltacheck)
	for n := range deltacheck {
		duplicateFiles = append(duplicateFiles, deltacheck[n].Fileinf)
	}
	fmt.Printf("duplicate file check completed in %s\n", time.Since(start).String())
	if autoclean {
		removedCount = duplicateAutoClean(duplicateFiles)
	}

	return
}

func duplicateAutoClean(duplicateFiles []FileData) (removedCount int) {
	hashlist := make(map[string][]int)

	for d := range duplicateFiles {
		hashlist[duplicateFiles[d].Sha1Sum] = append(hashlist[duplicateFiles[d].Sha1Sum], d)
	}
	for i := range hashlist {
		fmt.Println("cleaning sha duplicates", i)
		indexes := hashlist[i]
		var removalIndexes []int
		for _, v := range indexes {
			if strings.Contains(duplicateFiles[v].Name, "(") {
				removalIndexes = append(removalIndexes, v)
			}
		}

		switch {
		case len(hashlist[i])-len(removalIndexes) == 1:
			log.Println("successfully determined which copy of ", i, " to retain")

		case len(hashlist[i])-len(removalIndexes) > 1:
			fmt.Println("unable to simply determine which copy of ", i, " to retain, attempting to select just 1")
			fmt.Println(hashlist[i], removalIndexes)
			var nonparen []int
			fmt.Println("sanitizing list:")
			for _, z := range hashlist[i] {
				notFound := true
				for _, l := range removalIndexes {
					if l == z {
						notFound = false
					}
				}
				if notFound {
					nonparen = append(nonparen, z)
					fmt.Println(duplicateFiles[z].Name)
				}
			}
			retainFile := nonparen[len(nonparen)-1]
			nonparen = nonparen[:len(nonparen)-1]
			removalIndexes = append(removalIndexes, nonparen...)
			log.Println("retaining ", duplicateFiles[retainFile].Name)

		default:
			fmt.Println("unable to determine which copy of ", i, " to retain")
			continue

		}
		for _, r := range removalIndexes {
			pathparse := strings.Split(duplicateFiles[r].Name, "/")
			filename := pathparse[len(pathparse)-1]
			err := os.Rename(duplicateFiles[r].Name, "/Users/exa3946/Desktop/todelete/"+filename)
			if err != nil {
				fmt.Println("failed to remove ", duplicateFiles[r].Name)
				continue
			}
			removedCount++
		}
	}
	return
}

func fileWalk(root string) ([]Content, error) {
	var files []Content
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		var returnData Content
		returnData.Key = path
		returnData.Size = info.Size()
		files = append(files, returnData)
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v", files)
	return files, err
}

func sniff() {
	s3id := os.Getenv("S3ID")
	if s3id == "" {
		panic("no s3 id")
	}
	s3key := os.Getenv("S3KEY")
	if s3key == "" {
		panic("no s3 key")
	}
	bucket := aws.String("photo-buc")
	toUpload, err := fileWalk("/Users/exa3946/Documents/git/src/public/KowMangler/s3-synchorus")
	if err != nil {
		panic(err)
	}
	// region is required by aws package though not required by Ceph RGW.
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(s3id, s3key, ""),
		Endpoint:         aws.String("http://192.168.1.10:8000"),
		Region:           aws.String("us-east-1"),
		S3ForcePathStyle: aws.Bool(true),
	}
	newSession := session.New(s3Config)

	s3Client := s3.New(newSession)
	//buckets, err := s3Client.ListBuckets(&s3.ListBucketsInput{})

	// fmt.Printf("%+v\n", buckets)
	// objects, err := s3Client.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: buckets.Buckets[0].Name})
	// fmt.Printf("%+v", objects)
	// objectHead, err := s3Client.HeadObject(&s3.HeadObjectInput{Bucket: buckets.Buckets[0].Name, Key: objects.Contents[2].Key})
	// fmt.Printf("%+v", objectHead)
	// cparams := &s3.CreateBucketInput{
	// 	Bucket: bucket, // Required
	// }
	// _, err = s3Client.CreateBucket(cparams)
	// if err != nil {
	// 	// Print if any error.
	// 	fmt.Println(err.Error())
	// 	return
	// }
	//fmt.Printf("Successfully created bucket %s\n", *bucket)

	// Upload a new object "testfile.txt" with the string "S3 Compatible API"
	for _, p := range toUpload {
		uploadFileS3(s3Client, p.Key, bucket)
		break
	}
}

func uploadFileS3(s3Client *s3.S3, key string, bucket *string) {
	returninf, err := s3Client.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader("S3 Compatible API"),
		Bucket: bucket,
		Key:    &key,
	})
	if err != nil {
		fmt.Printf("Failed to upload object %s/%s, %s\n", *bucket, key, err.Error())
		return
	}
	fmt.Printf("Successfully uploaded key %s\n", key)
	fmt.Printf("%+v", returninf)

	//Get Object
	_, err = s3Client.GetObject(&s3.GetObjectInput{
		Bucket: bucket,
		Key:    &key,
	})
	if err != nil {
		fmt.Println("Failed to download file", err)
		return
	}
	fmt.Printf("Successfully Downloaded key %s\n", key)
}
