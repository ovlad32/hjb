package iixs

import (
	"fmt"
	"io"
	"sort"

	"github.com/ovlad32/hjb/misc/serde"
	"github.com/pkg/errors"
)

const MAX_COLUMN_NAME_LENGTH uint64 = 1000

type vrc struct {
	columnID string
	rns      IRnStorage
	rnsf     IRnStorageFactory
}

type Rcs struct {
	s    []vrc
	rnsf IRnStorageFactory
}

func (v *vrc) addRow(rn int) vrc {
	if v.rns == nil {
		v.rns = v.rnsf.RnStorage()
	}
	v.rns.Add(rn)
	return *v
}

func (rc vrc) WriteTo(writer io.Writer) (total int64, err error) {
	var ni64 int64
	cLen := uint64(len(rc.columnID))
	if cLen > MAX_COLUMN_NAME_LENGTH {
		err = errors.Errorf("cLen {%s} > MAX_COLUMN_NAME_LENGTH", cLen)
		return
	}
	total, err = serde.StringWriteTo(writer, rc.columnID)
	if err != nil {
		err = errors.Wrap(err, "could not serialize column identifier")
		return
	}
	ni64, err = rc.rns.WriteTo(writer)
	if err != nil {
		err = errors.Wrap(err, "couldn't serialize RnStorage")
		return
	}
	total += ni64
	return
}

func (rc *vrc) ReadFrom(reader io.Reader) (total int64, err error) {
	var ni64 int64
	total, err = serde.StringReadFrom(&rc.columnID, reader)
	if err != nil {
		err = errors.Wrap(err, "couldn't deserialize column identifier")
		return
	}
	if rc.rns == nil {
		rc.rns = rc.rnsf.RnStorage()
	}
	ni64, err = rc.rns.ReadFrom(reader)
	if err != nil {
		err = errors.Wrap(err, "couldn't deserialize RnStorage")
		return
	}
	total += ni64
	return
}

func (rcs Rcs) Len() int {
	return len(rcs.s)
}
func (rcs Rcs) Less(i, j int) bool {
	return rcs.s[i].columnID < rcs.s[j].columnID
}
func (rcs Rcs) Swap(i, j int) {
	rcs.s[i], rcs.s[j] = rcs.s[j], rcs.s[i]
}

func (rcs Rcs) indexOf(cid string) (ix int, found bool) {
	ix = sort.Search(len(rcs.s), func(i int) bool {
		return rcs.s[i].columnID >= cid
	})
	// if found indeed
	found = ix < len(rcs.s) && rcs.s[ix].columnID == cid
	return
}

func (rcs Rcs) indexOfWithPanic(cid string) (ix int) {
	var found bool
	if ix, found = rcs.indexOf(cid); !found {
		panic(fmt.Sprintf("RCS column ID '%v' has not been found!", cid))
	}
	return
}

func (rcs *Rcs) Append(rn int, cid string) error {
	if rcs.s == nil {
		rcs.s = make([]vrc, 0, 1)
		rca := vrc{
			columnID: cid,
			rnsf:     rcs.rnsf,
		}
		rcs.s = append(rcs.s, rca.addRow(rn))
		return nil
	}
	ix, found := rcs.indexOf(cid)
	if found {
		rcs.s[ix].addRow(rn)
	} else {
		tail := rcs.s[len(rcs.s)-1].columnID
		rca := vrc{
			columnID: cid,
			rnsf:     rcs.rnsf,
		}
		rcs.s = append(rcs.s, rca.addRow(rn))
		if tail > cid {
			sort.Sort(rcs)
		}
	}
	return nil
}

func (rcs Rcs) ColumnIDs() []string {
	c := make([]string, 0, len(rcs.s))
	for i := range rcs.s {
		c = append(c, rcs.s[i].columnID)
	}
	return c
}
func (rcs Rcs) ColumnIDSet() map[string]struct{} {
	c := make(map[string]struct{}, len(rcs.s))
	for i := range rcs.s {
		c[rcs.s[i].columnID] = struct{}{}
	}
	return c
}
func (rcs Rcs) TotalColumns() int {
	return len(rcs.s)
}

func (rcs Rcs) RowStorage(i int) IRnStorage {
	return rcs.s[i].rns
}

func (rcs Rcs) TotalRows(i int) int {
	return int(rcs.s[i].rns.Cardinality())
}

func (rcs Rcs) RowIteratorByColumn(c string) IRnIterator {
	return rcs.RowStorage(rcs.indexOfWithPanic(c)).Iterator()
}

func (rcs Rcs) TotalRowsByColumn(c string) int {
	return rcs.TotalRows(rcs.indexOfWithPanic(c))
}

func (rcs Rcs) WriteTo(w io.Writer) (n int64, err error) {
	n, err = serde.SliceWriteTo(w, rcs.s)
	if err != nil {
		err = errors.Wrap(err, "could not serialize Rc slice")
	}
	return
}

func (rcs *Rcs) ReadFrom(r io.Reader) (n int64, err error) {
	fMake := func(l int) interface{} {
		rcs.s = make([]vrc, l)
		for i := range rcs.s {
			rcs.s[i].rnsf = rcs.rnsf
		}
		return rcs.s
	}
	n, err = serde.SliceReadFrom(fMake, r)
	if err != nil {
		err = errors.Wrap(err, "could not deserialize Rc slice")
	}
	return
}

/*
func (rc rc) WriteTo(w io.Writer) (n int64, err error) {
	var ni int
	var ni64 int64
	var buff [8]byte
	buffSlice := buff[:]

	cLen := uint64(len(rc.column))
	if cLen > MAX_COLUMN_NAME_LENGTH {
		log.Fatalf(" cLen {%s} > MAX_COLUMN_NAME_LENGTH", cLen)
	}
	binary.BigEndian.PutUint64(buffSlice, cLen)
	ni, err = w.Write(buffSlice)
	if err != nil {
		log.Fatal("Write len(rc.column)")
	}
	n += int64(ni)

	ni, err = io.WriteString(w, rc.column)
	if err != nil {
		log.Fatal("Write rc.column")
	}
	n += int64(ni)

	ni64, err = rc.rns.WriteTo(w)
	if err != nil {
		log.Fatal("Write rc.rns")
	}
	n += ni64
	return n, nil
}

func (rc rc) ReadFrom(r io.Reader) (n int64, err error) {
	var ni int
	var ni64 int64

	var buff [8]byte
	buffSlice := buff[:]
	ni, err = r.Read(buffSlice)
	if err != nil {
		log.Fatal("Read len(rc.column)")
	}
	n += int64(ni)

	cLen := binary.BigEndian.Uint64(buffSlice)
	if cLen > MAX_COLUMN_NAME_LENGTH {
		log.Fatalf(" cLen {%s} > MAX_COLUMN_NAME_LENGTH", cLen)
	}
	if cLen > MAX_COLUMN_NAME_LENGTH {
		log.Fatalf(" cLen {%s} < 0", cLen)
	}

	builder := new(strings.Builder)
	builder.Grow(int(cLen))
	ni64, err = io.CopyN(builder, r, int64(cLen))
	if err != nil {
		log.Fatal("Read rc.column")
	}
	n += ni64

	ni64, err = rc.rns.ReadFrom(r)
	if err != nil {
		log.Fatal("Read rc.rns")
	}
	n += ni64
	return n, nil
}

*/

/*
func (rcs Rcs) WriteTo(w io.Writer) (n int64, err error) {
	var ni int
	var ni64 int64
	var buff [8]byte
	buffSlice := buff[:]

	sLen := uint64(len(rcs.s))
	binary.BigEndian.PutUint64(buffSlice, sLen)
	ni, err = w.Write(buffSlice)
	if err != nil {
		log.Fatal("Write len(rcs)")
	}
	n += int64(ni)
	for i := range rcs.s {
		ni64, err = rcs.s[i].WriteTo(w)
		if err != nil {
			log.Fatal("Write rcs.s[i]")
		}
		n += ni64
	}
	return n, nil
}


func (rcs *Rcs) ReadFrom(r io.Reader) (n int64, err error) {
	var ni int
	var ni64 int64

	var buff [8]byte
	buffSlice := buff[:]
	ni, err = r.Read(buffSlice)
	if err != nil {
		log.Fatal("Read len(rc.column)")
	}
	n += int64(ni)

	cLen := binary.BigEndian.Uint64(buffSlice)
	rcs.s = make([]rc, 0, cLen)
	for i := uint64(0); i < cLen; i++ {
		a := rc{}
		ni64, err = a.ReadFrom(r)
		if err != nil {
			log.Fatal("Read rc[i]")
		}
		n += ni64
		rcs.s = append(rcs.s, a)
	}
	return n, nil
}*/
