package iixs

import (
	"io"

	"github.com/RoaringBitmap/roaring"
)

type rnStorageFactory struct{}

func NewRnStorageFactory() IRnStorageFactory {
	return rnStorageFactory{}
}
func (r rnStorageFactory) RnStorage() IRnStorage {
	return NewRoaringBitmapStorage()
}

type roaringBitmapStorage struct {
	s *roaring.Bitmap
}
type rowNumberIterator struct {
	peekable roaring.IntPeekable
}

func NewRoaringBitmapStorage() roaringBitmapStorage {
	return roaringBitmapStorage{
		s: roaring.NewBitmap(),
	}
}

func (rs roaringBitmapStorage) Add(a int) {
	rs.s.AddInt(a)
}
func (rs roaringBitmapStorage) Contains(a int) bool {
	return rs.s.ContainsInt(a)
}

func (rs roaringBitmapStorage) Iterator() IRnIterator {
	return rowNumberIterator{
		peekable: rs.s.Iterator(),
	}
}

func (rs roaringBitmapStorage) Cardinality() uint {
	return uint(rs.s.GetCardinality())
}

func (rs roaringBitmapStorage) WriteTo(w io.Writer) (int64, error) {
	return rs.s.WriteTo(w)
}

func (rs roaringBitmapStorage) ReadFrom(r io.Reader) (int64, error) {
	return rs.s.ReadFrom(r)
}
func (rs roaringBitmapStorage) Or(a IRnStorage) {
	b, ok := a.(*roaringBitmapStorage)
	if ok {
		rs.s.Or(b.s)
	}
}
func (rs roaringBitmapStorage) OrBitmap(a *roaring.Bitmap) {
	rs.s.Or(a)
}

func (ri rowNumberIterator) HasNext() bool {
	return ri.peekable.HasNext()
}

func (ri rowNumberIterator) Next() int {
	return int(ri.peekable.Next())
}
func (ri rowNumberIterator) PeekNext() int {
	return int(ri.peekable.PeekNext())
}
func (ri rowNumberIterator) AdvanceIfNeeded(minValue int) {
	ri.peekable.AdvanceIfNeeded(uint32(minValue))
}
