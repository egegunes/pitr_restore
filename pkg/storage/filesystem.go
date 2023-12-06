package storage

import "github.com/percona/pitr_restore/pkg/binlog"

type FilesystemStorage struct {
	Directory string
}

func NewFilesystemStorage(directory string) BinlogStorage {
	return &FilesystemStorage{
		Directory: directory,
	}
}

func (s *FilesystemStorage) ListBinlogs() ([]binlog.Binlog, error) {
	return nil, nil
}

func (s *FilesystemStorage) DownloadBinlog(blog binlog.Binlog) ([]byte, error) {
	return nil, nil
}
