package iixs

import (
	"io"
	"sync"

	"github.com/ovlad32/hjb/misc/serde"
	"github.com/pkg/errors"
)

type dataSparnessStorage struct {
	onceInit    sync.Once
	rnsf        IRnStorageFactory
	columnState map[string]IRnStorage
}

func NewDataSparnessStorage(rnsf IRnStorageFactory) *dataSparnessStorage {
	return &dataSparnessStorage{
		rnsf: rnsf,
	}
}
func (s *dataSparnessStorage) init() error {
	s.columnState = make(map[string]IRnStorage)
	return nil
}

func (s *dataSparnessStorage) Add(columnID string, rowNumber int) (err error) {
	s.onceInit.Do(func() { err = s.init() })
	var state IRnStorage
	var found bool
	if state, found = s.columnState[columnID]; !found {
		state = s.rnsf.RnStorage()
		s.columnState[columnID] = state
	}
	state.Add(rowNumber)
	return
}

func (s *dataSparnessStorage) EmptyValuesCounts(consume func(string, uint) error) error {
	for c, state := range s.columnState {
		err := consume(c, state.Cardinality())
		if err != nil {
			errors.WithStack(err)
			return err
		}
	}
	return nil
}

func (s *dataSparnessStorage) CheckColumn(columnId string, rowNumber int) (found bool, err error) {
	var state IRnStorage
	if state, found = s.columnState[columnId]; found && state != nil {
		found = state.Contains(rowNumber)
	}
	return
}

func (s *dataSparnessStorage) CheckRowNumber(rowNumber int) (res map[string]bool, err error) {
	res = make(map[string]bool)
	for columnID, state := range s.columnState {
		found := state.Contains(rowNumber)
		if found {
			res[columnID] = true
		}
	}
	return
}

func (s *dataSparnessStorage) WriteTo(w io.Writer) (total int64, err error) {
	var mLen, n64 int64
	mLen = int64(len(s.columnState))
	n64, err = serde.IntWriteTo(w, mLen)
	if err != nil {
		err = errors.Wrapf(err, "# of entries")
		return
	}
	total += n64

	for columnID, state := range s.columnState {
		n64, err = serde.StringWriteTo(w, columnID)
		if err != nil {
			err = errors.Wrapf(err, "entry key = column")
			return
		}
		n64, err = state.WriteTo(w)
		if err != nil {
			err = errors.Wrapf(err, "entry value: state.data")
			return
		}
		total += n64
	}
	return
}

func (s *dataSparnessStorage) ReadFrom(r io.Reader) (total int64, err error) {
	var mLen int64
	var ni64 int64
	s.onceInit.Do(func() { err = s.init() })

	ni64, err = serde.IntReadFrom(&mLen, r)
	if err != nil {
		err = errors.Wrapf(err, "Couldn't read storage map length")
		return
	}
	total += ni64

	for i := 0; i < int(mLen); i++ {
		var columnID string
		ni64, err = serde.StringReadFrom(&columnID, r)
		if err != nil {
			err = errors.Wrapf(err, "Couldn't read storage map key")
			return
		}

		state := s.rnsf.RnStorage()
		ni64, err = state.ReadFrom(r)
		if err != nil {
			err = errors.Wrapf(err, "Couldn't read state data")
			return
		}
		s.columnState[columnID] = state
	}
	return
}
