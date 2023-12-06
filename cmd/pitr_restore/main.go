package main

import (
	"bytes"
	"log"
	"os"
	"os/exec"

	"github.com/percona/pitr_restore/pkg/binlog"
	"github.com/percona/pitr_restore/pkg/storage"
)

type task struct {
	buf  []byte
	name string
}

func main() {
	// initialize storage
	storage := storage.NewS3Storage("http://minio-service:9000", "operator-testing", "some-access-key", "some-secret-key", "binlog_")

	binlogs, err := storage.ListBinlogs()
	if err != nil {
		log.Fatalf("Failed to list binlogs: %s", err)
	}

	for _, blog := range binlogs {
		if blog.Size == 0 {
			continue
		}
		log.Printf("Working on binlog %s, size: %d", blog.Name, blog.Size)
		if err := processBinlog(storage, blog); err != nil {
			log.Fatalf("Failed to process binlog %s: %s", blog.Name, err)
		}
	}

	log.Println("DONE")

	os.Exit(0)
}

func processBinlog(storage storage.BinlogStorage, blog binlog.Binlog) error {
	contents, err := storage.DownloadBinlog(blog)
	if err != nil {
		return err
	}

	log.Printf("Processing binlog %s", blog.Name)

	mysqlbinlog := exec.Command("mysqlbinlog", "-vv", "-")
	mysqlbinlog.Stdin = bytes.NewReader(contents)
	mysqlbinlog.Stdout = os.Stdout
	mysqlbinlog.Stderr = os.Stderr

	err = mysqlbinlog.Run()
	if err != nil {
		return err
	}

	log.Printf("Done processing binlog %s", blog.Name)

	return nil
}
