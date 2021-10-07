package meta

import (
	"strconv"
	"strings"
	"sync"
)

//TODO: Refactor it!!!

type TextRowHandlerAdapter struct {
	iRowHandler
	td        *TableDescription
	sepOnce   sync.Once
	separator []byte
}

func toByte(sValue string) byte {
	// remove 0x suffix if found in the input string
	var base int = 10
	var cleaned string
	if cleaned = strings.Replace(sValue, "0x", "", -1); cleaned != sValue {
		base = 16
	} else if sValue[0] == '0' {
		cleaned = sValue
		base = 8
	}
	result, _ := strconv.ParseUint(cleaned, base, 64)
	return byte(result)
}
func (d *TextRowHandlerAdapter) Separator() []byte {
	d.sepOnce.Do(func() {
		d.separator = []byte{toByte(d.td.ColumnSeparator)}
	})
	return d.separator
}
func NewTextRowHandlerAdapter(rh iRowHandler, td *TableDescription) *TextRowHandlerAdapter {
	return &TextRowHandlerAdapter{
		iRowHandler: rh,
		td:          td,
	}
}
