package iixs

import (
	"io"

	"github.com/RoaringBitmap/roaring"
)

type ITX int

const (
	COMMIT ITX = iota
	ROLLBACK
)

type IRnStorage interface {
	Add(int)
	Or(IRnStorage)
	OrBitmap(*roaring.Bitmap) //quick hack
	Contains(a int) bool
	Cardinality() uint
	Iterator() IRnIterator
	io.WriterTo
	io.ReaderFrom
}

type IRnIterator interface {
	HasNext() bool
	Next() int
	PeekNext() int
	AdvanceIfNeeded(minval int)
}

type IRnStorageFactory interface {
	RnStorage() IRnStorage
}

/*
type IRcStorageFactory interface {
	RcStorage() IRcStorage
}
*/
type PushFunc = func(Rcs) error
type TxFunc = func(ITX) PushFunc

type IRcUpdater interface {
	Update(string) (Rcs, TxFunc, error)
	//	io.WriterTo
}
type IRcFinder interface {
	Find(string) (Rcs, bool, error)
	//	io.ReaderFrom
}

type IRcStorage interface {
	IRcUpdater
	IRcFinder
}

/*
type IRcs interface {
	Columns() []string
	TotalRows(string) uint
	RowIterator(string) IRnIterator
}
*/

/*
type IAppender interface {
	Append(rn int, cl string) error
}
*/
