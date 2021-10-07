package iixs

/*
type RowColumnSliceMode int
const (
	Impl IrcnsMode = iota,
	Mock,
)

type RcnsFactory struct{}
func (r RcnsFactory) With(iMode string ) IRowColumnFactory {
	switch
}

type IRowColumnFactory interface {
	NewRcsN(int) IRcStorage
	//NewRc(string) IRcStorageAdder,
	NewRnStorage() IRnStorage
}

type RowColumnImplFactory struct{}

func (f RowColumnImplFactory) NewRcsN(cap int) Rcs {
	rcs := Rcs{
		s: make([]rc, 0, cap),
	}
	return rcs
}

func (f RowColumnImplFactory) NewRcs() Rcs {
	return f.NewRcsN(1)
}

func (f RowColumnImplFactory) NewRc(column string) rc {
	return rc{
		column: column,
		rns:    f.NewRnStorage(),
	}
}

func (f RowColumnImplFactory) NewRnStorage() IRnStorage {
	return roaringBitmapStorage{
		s: roaring.NewBitmap(),
	}
}
*/
