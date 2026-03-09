package domain

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

type Log struct {
	FileName string
	fp       *os.File
}

func (log *Log) Open() (err error) {
	log.fp, err = createFileSync(log.FileName)
	return
}

func createFileSync(file string) (*os.File, error) {
	fp, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := fp.Sync(); err != nil {
		return nil, err
	}
	if err := syncDir(file); err != nil {
		_ = fp.Close()
		return nil, err
	}
	return fp, err
}

func syncDir(file string) error {
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirFd, err := syscall.Open(filepath.Dir(file), flags, 0o644)
	if err != nil {
		return err
	}
	defer syscall.Close(dirFd)
	return syscall.Fsync(dirFd)
}

func (log *Log) Close() error {
	return log.fp.Close()
}

func (log *Log) Write(ent *Entry) error {
	if _, err := log.fp.Write(ent.Encode()); err != nil {
		return err
	}
	return log.fp.Sync()
}

func (log *Log) Read(ent *Entry) (eof bool, err error) {
	err = ent.Decode(log.fp)
	if errors.Is(err, io.EOF) {
		return true, nil
	} else if err != nil {
		return false, err
	} else {
		return false, nil
	}
}
