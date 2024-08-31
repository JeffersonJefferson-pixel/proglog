package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offsetWidth uint64 = 4
	posWidth    uint64 = 8
	entWidth           = offsetWidth + posWidth
)

// index entries contains the record's offset and its position in the store file
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
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
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

// takes an offset and returns the record's position in the store.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	// given offset is relative to the segment's base offset
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	// offset
	out = enc.Uint32(i.mmap[pos : pos+offsetWidth])
	// position
	pos = enc.Uint64(i.mmap[pos+offsetWidth : pos+entWidth])
	return out, pos, nil
}

// append the given offest and position to the index.
func (i *index) Write(offset uint32, pos uint64) error {
	// check fi there is space to write entry
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	// encode offset and position and write to mmap
	enc.PutUint32(i.mmap[i.size:i.size+offsetWidth], offset)
	enc.PutUint64(i.mmap[i.size+offsetWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
