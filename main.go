package main

import (
	"crypto/sha1"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/kothar/go-backblaze.v0"
)

type Content struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

var (
	localfiles  []Content
	imageBucket string
	videoBucket string
)

type FileData struct {
	Name    string
	Sha1Sum string
}

func main() {
	id := os.Getenv("B2ID")
	key := os.Getenv("B2SECRET")

	b2cred, err := backblaze.NewB2(backblaze.Credentials{
		KeyID:          id,
		ApplicationKey: key,
	})

	if err != nil {
		panic(err)
	}
	buckets, err := b2cred.ListBuckets()

	if err != nil {
		log.Fatalln(err)
	}
	for _, b := range buckets {
		fmt.Println(b.Name)
		f, err := b.ListFileNamesWithPrefix("", 1000, "", "")
		if err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println(f.Files)
		}
	}

	filesToUpload, err := fileWalk(os.Getenv("DATAPATH"))
	if err != nil {
		panic(err)
	}

	for _, f := range filesToUpload {
		dat, err := os.ReadFile(f.Key)
		if err != nil {
			panic(err)
		}
		sha := sha1Sum(dat)
		metadata := make(map[string]string)
		fileReader, err := os.Open(f.Key)
		if err != nil {
			panic(err)
		}
		metadata["X-Bz-Content-Sha1"] = sha
		metadata["large_file_sha1"] = sha
		fmt.Println(metadata, f.Key)
		if strings.HasSuffix(f.Key, "mov") {
			buckets[1].UploadFile(filepath.Base(f.Key), metadata, fileReader)
		} else {
			buckets[0].UploadFile(filepath.Base(f.Key), metadata, fileReader)
		}
		fileReader.Close()
	}

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

// Pricing Organized by API Calls
// These details are provided for developers and programmers

// Transactions Class A
// Costs: Free

// B2 Native API
// S3 Compatible API
// b2_cancel_large_file
// Abort Multipart Upload
// b2_delete_bucket
// CreateMultipartUpload
// b2_delete_file_version
// CompleteMultipartUpload
// b2_delete_key
// DeleteBucket
// b2_finish_large_file
// DeleteObject
// b2_get_upload_part_url
// DeleteObjects
// b2_get_upload_url
// PutObject
// b2_hide_file
// PutObjectLegalHold
// b2_start_large_file
// PutObjectLockConfiguration
// b2_update_file_legal_hold
// PutObjectRetention
// b2_update_file_retention
// UploadPart
// b2_upload_file
// b2_upload_part

// Note: There is no charge to send data (upload) to Backblaze B2. Backblaze does not charge bandwidth fees and does not charge for b2_upload_file calls. Once uploaded, storage charges apply to all data after the first 10 Gigabytes in your account at the rate of $.005/GB/month.

// Transactions Class B
// Cost: The first 2,500 of these calls are free each day, then $0.004 per 10,000

// B2 Native API
// S3 Compatible API
// b2_download_file_by_id
// GetObject
// b2_download_file_by_name
// GetObjectLegalHold
// b2_get_file_info
// GetObjectLockConfiguration
// GetObjectRetention
// HeadObject

// Note: In addition to the cost of each API call, there is a cost of downloading data specific to b2_download_file_by_id and b2_download_file_by_name. The first 1 GByte downloaded each day is free, then the amount over 1 GByte costs $0.01/GByte. Each day starts at 00:00 UTC.

// Transactions Class C
// Cost: The first 2,500 of these calls are free each day, then $0.004 per 1,000

// B2 Native API
// S3 Compatible API
// b2_authorize_account
// CopyObject (Put Object Copy)
// b2_copy_file
// CreateBucket
// b2_copy_part
// DeleteBucketCors
// b2_create_bucket
// DeleteBucketEncryption
// b2_create_key
// GetBucketAcl (List Objects)
// b2_get_download_authorization
// GetBucketCors
// b2_list_buckets
// GetBucketEncryption
// b2_list_file_names
// GetBucketLocation
// b2_list_file_versions
// GetBucketVersioning
// b2_list_keys
// GetObjectAcl
// b2_list_parts
// HeadBucket
// b2_list_unfinished_large_files
// ListBuckets
// b2_update_bucket
// ListMultipartUploads
// ListObjectsV2
// ListObjectVersions
// ListParts
// PutBucketAcl
// PutBucketCors
// PutBucketEncryption
// PutObjectAcl
// UploadPartCopy
