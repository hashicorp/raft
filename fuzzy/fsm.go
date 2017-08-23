package fuzzy

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/adler32"
	"io"
	"os"

	"github.com/hashicorp/raft"
)

type logHash struct {
	lastHash []byte
}

func (l *logHash) Add(d []byte) {
	hasher := adler32.New()
	hasher.Write(l.lastHash)
	hasher.Write(d)
	l.lastHash = hasher.Sum(nil)
}

type applyItem struct {
	index uint64
	term  uint64
	data  []byte
}

func (a *applyItem) set(l *raft.Log) {
	a.index = l.Index
	a.term = l.Term
	a.data = make([]byte, len(l.Data))
	copy(a.data, l.Data)
}

type fuzzyFSM struct {
	logHash
	lastTerm  uint64
	lastIndex uint64
	applied   []applyItem
}

func (f *fuzzyFSM) Apply(l *raft.Log) interface{} {
	if l.Index <= f.lastIndex {
		panic(fmt.Errorf("fsm.Apply received log entry with invalid Index %v (lastIndex we saw was %d)", l, f.lastIndex))
	}
	if l.Term < f.lastTerm {
		panic(fmt.Errorf("fsm.Apply received log entry with invalid Term %v (lastTerm we saw was %d)", l, f.lastTerm))
	}
	f.lastIndex = l.Index
	f.lastTerm = l.Term
	f.Add(l.Data)
	f.applied = append(f.applied, applyItem{})
	f.applied[len(f.applied)-1].set(l)
	return nil
}

func (f *fuzzyFSM) WriteTo(fn string) error {
	fw, err := os.Create(fn)
	if err != nil {
		return err
	}
	defer fw.Close()
	w := bufio.NewWriter(fw)
	defer w.Flush()
	for _, i := range f.applied {
		fmt.Fprintf(w, "%d.%8d: %X\n", i.term, i.index, i.data)
	}
	return nil
}

func (f *fuzzyFSM) Snapshot() (raft.FSMSnapshot, error) {
	s := *f
	return &s, nil
}

func (f *fuzzyFSM) Restore(r io.ReadCloser) error {
	err := binary.Read(r, binary.LittleEndian, &f.lastTerm)
	if err == nil {
		err = binary.Read(r, binary.LittleEndian, &f.lastIndex)
	}
	if err == nil {
		f.lastHash = make([]byte, adler32.Size)
		_, err = r.Read(f.lastHash)
	}
	return err
}

func (f *fuzzyFSM) Persist(sink raft.SnapshotSink) error {
	err := binary.Write(sink, binary.LittleEndian, f.lastTerm)
	if err == nil {
		err = binary.Write(sink, binary.LittleEndian, f.lastIndex)
	}
	if err == nil {
		_, err = sink.Write(f.lastHash)
	}
	return err
}

func (f *fuzzyFSM) Release() {
}
