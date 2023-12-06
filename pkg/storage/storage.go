package storage

import (
	"github.com/percona/pitr_restore/pkg/binlog"
)

type BinlogStorage interface {
	// ListBinlogs lists binlogs from storage
	ListBinlogs() ([]binlog.Binlog, error)

	// DownloadBinlog downloads binlog from storage
	DownloadBinlog(binlog.Binlog) ([]byte, error)
}
