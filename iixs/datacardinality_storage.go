package iixs

import (
	"hash/fnv"
	"io"
	"sync"

	hll "github.com/clarkduvall/hyperloglog"
	"github.com/ovlad32/hjb/misc/serde"
	"github.com/pkg/errors"
)

const PRECISION = uint8(14)

type dataCardinalityStorage struct {
	precision   uint8
	onceInit    sync.Once
	columnState map[string]*hll.HyperLogLogPlus //TODO: Abstract the HLL
}

func NewColumnDataCardianlityStorage() *dataCardinalityStorage {
	return &dataCardinalityStorage{}
}

func (s *dataCardinalityStorage) Add(columnID string, value string) (err error) {
	s.onceInit.Do(func() {
		s.precision = PRECISION
		s.columnState = make(map[string]*hll.HyperLogLogPlus)
	})
	var state *hll.HyperLogLogPlus
	var found bool
	if state, found = s.columnState[columnID]; !found {
		state, err = hll.NewPlus(s.precision)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		s.columnState[columnID] = state
	}
	h := fnv.New64()
	_, err = h.Write([]byte(value))
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	state.Add(h)
	return
}

func (s dataCardinalityStorage) Cardinality(columnID string) (n uint, err error) {
	if state, found := s.columnState[columnID]; !found {
		err = errors.Errorf("No HLL state registered for column ID %v", columnID)
	} else {
		n = uint(state.Count())
	}
	return
}

func (s dataCardinalityStorage) Cardinalities(consume func(string, uint) error) error {
	for c, state := range s.columnState {
		err := consume(c, uint(state.Count()))
		if err != nil {
			errors.WithStack(err)
			return err
		}
	}
	return nil
}

func (s dataCardinalityStorage) WriteTo(w io.Writer) (total int64, err error) {
	var mLen, n64 int64
	mLen = int64(len(s.columnState))
	n64, err = serde.IntWriteTo(w, mLen)
	if err != nil {
		err = errors.Wrapf(err, "# of entries")
		return
	}
	total += n64
	n64, err = serde.IntWriteTo(w, int64(s.precision))
	if err != nil {
		err = errors.Wrapf(err, "Couldn't initialize hll state")
		return
	}
	total += n64

	for columnID, state := range s.columnState {
		n64, err = serde.StringWriteTo(w, columnID)
		if err != nil {
			err = errors.Wrapf(err, "entry key = column")
			return
		}
		b, erre := state.GobEncode()
		if erre != nil {
			err = errors.Wrapf(erre, "entry value:  state.GobEncode")
			return
		}
		n64, err = serde.IntWriteTo(w, int64(len(b)))
		if erre != nil {
			err = errors.Wrapf(erre, "entry value: state.length")
			return
		}
		n, erre := w.Write(b)
		if erre != nil {
			err = errors.Wrapf(erre, "entry value: state.data")
			return
		}
		total += int64(n)
	}
	return
}

func (s *dataCardinalityStorage) ReadFrom(r io.Reader) (total int64, err error) {
	var mLen, slen, precision int64
	var ni64 int64
	s.columnState = make(map[string]*hll.HyperLogLogPlus)
	ni64, err = serde.IntReadFrom(&mLen, r)
	if err != nil {
		err = errors.Wrapf(err, "Couldn't read storage map length")
		return
	}
	total += ni64

	ni64, err = serde.IntReadFrom(&precision, r)
	if err != nil {
		err = errors.Wrapf(err, "Couldn't read storage precision")
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

		ni64, err = serde.IntReadFrom(&slen, r)
		if err != nil {
			err = errors.Wrapf(err, "Couldn't read storage map value's data length")
			return
		}
		b := make([]byte, slen)
		var ni int
		ni, err = r.Read(b)
		if err != nil {
			err = errors.Wrapf(err, "Couldn't read storage data")
			return
		}
		total += int64(ni)
		var state *hll.HyperLogLogPlus

		state, err = hll.NewPlus(uint8(precision))
		if err != nil {
			err = errors.Wrapf(err, "Couldn't initialize hll state")
			return
		}

		err = state.GobDecode(b)
		if err != nil {
			err = errors.Wrapf(err, "Couldn't decode loaded data")
			return
		}
		s.columnState[columnID] = state
	}
	return
}
