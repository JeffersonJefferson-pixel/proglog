package log

import (
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offsetWidth uint64 = 4
	posWidth    uint64 = 8
	entWidth           = offsetWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// create an index for a given file
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	// grpw the file to max index size
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxindexBytes)); err != nil {
		return nil, err
	}
	// mmap index file
	if idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}
	return idx, nil
}

// close index file
func (i *index) Close() error {
	// make sure mmap sync data to file
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	// persist file to stable storage
	if err := i.file.Sync(); err != nil {
		return err
	}
	// truncate file to actual size
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}
