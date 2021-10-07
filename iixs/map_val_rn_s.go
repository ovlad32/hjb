package iixs

import (
	"encoding/json"
	"io"
	"sort"

	"github.com/ovlad32/hjb/misc/serde"
	"github.com/pkg/errors"
)

type mMap map[string]Rcs

type indexerMemStorage struct {
	rnsf IRnStorageFactory
	mem  mMap
}

func NewMemIndexerStorage(rnsf IRnStorageFactory) indexerMemStorage {
	return indexerMemStorage{
		rnsf: rnsf,
		mem:  make(mMap),
	}
}

func (s indexerMemStorage) Update(v string) (rcs Rcs, txFunc TxFunc, err error) {
	txFunc = func(itx ITX) PushFunc {
		if itx == COMMIT {
			return func(ircs Rcs) error {
				s.mem[v] = ircs
				return nil
			}
		}
		return nil
	}
	var found bool
	rcs, found = s.mem[v]
	if !found {
		rcs = s.createRcs()
	}
	return rcs, txFunc, nil
}
func (s indexerMemStorage) Find(v string) (rcs Rcs, found bool, err error) {
	rcs, found = s.mem[v]
	return rcs, found, nil
}

func (s indexerMemStorage) InspectTo(w io.Writer) error {
	d := json.NewEncoder(w)
	return d.Encode(s)
}
func (s indexerMemStorage) createRcs() Rcs {
	return Rcs{
		rnsf: s.rnsf,
	}
}

func (s indexerMemStorage) sortAndWriteTo(w io.Writer) (total int64, err error) {
	var ni64 int64

	ni64, err = serde.IntWriteTo(w, int64(len(s.mem)))
	if err != nil {
		err = errors.Wrap(err, "couldn't write memStorage capacity")
		return
	}
	total += ni64
	type tkv struct {
		k string
		v Rcs
	}
	tkvs := make([]tkv, 0, len(s.mem))

	for k, rcs := range s.mem {
		tkvs = append(tkvs, tkv{k, rcs})
	}

	sort.Slice(tkvs, func(i, j int) bool {
		return tkvs[i].k < tkvs[j].k
	})
	for _, tkv := range tkvs {
		ni64, err = serde.StringWriteTo(w, tkv.k)
		if err != nil {
			err = errors.Wrapf(err, "couldn't write a memStorage key value %v", tkv.k)
			return
		}
		total += ni64
		//ni64, err = serde.SliceWriteTo(w, v)
		ni64, err = tkv.v.WriteTo(w)
		if err != nil {
			err = errors.Wrapf(err, "couldn't write a memStorage slice value for the key %v", tkv.k)
			return
		}
		total += ni64
	}
	return
}
func (s indexerMemStorage) WriteTo(w io.Writer) (total int64, err error) {
	var ni64 int64

	ni64, err = serde.IntWriteTo(w, int64(len(s.mem)))
	if err != nil {
		err = errors.Wrap(err, "couldn't write memStorage capacity")
		return
	}
	total += ni64
	for k, rcs := range s.mem {
		ni64, err = serde.StringWriteTo(w, k)
		if err != nil {
			err = errors.Wrapf(err, "couldn't write a memStorage key value %v", k)
			return
		}
		total += ni64
		//ni64, err = serde.SliceWriteTo(w, v)
		ni64, err = rcs.WriteTo(w)
		if err != nil {
			err = errors.Wrapf(err, "couldn't write a memStorage slice value for the key %v", k)
			return
		}
		total += ni64
	}
	return
}

func (s indexerMemStorage) ReadFrom(r io.Reader) (total int64, err error) {
	var ni64 int64
	var count int64
	ni64, err = serde.IntReadFrom(&count, r)
	if err != nil {
		err = errors.Wrap(err, "couldn't deseralize the storage capacity")
		return
	}
	total += ni64
	for i := 0; i < int(count); i++ {
		var k string
		ni64, err = serde.StringReadFrom(&k, r)
		if err != nil {
			err = errors.Wrap(err, "couldn't deserialize storage entry key")
			return
		}
		total += ni64
		rcs := s.createRcs()
		ni64, err = rcs.ReadFrom(r)
		if err != nil {
			err = errors.Wrapf(err, "couldn't deserialize storage entry values with key value %v", k)
			return
		}
		s.mem[k] = rcs
	}
	return
}
