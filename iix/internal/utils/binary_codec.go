package utils

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
)

const uint64Size int = 8
const trueChar byte = 'T'
const falseChar byte = 'F'

type BinaryCodec struct{}

func (t BinaryCodec) EncodeFrom(w io.Writer, iv interface{}) (n int, err error) {

	var order = binary.LittleEndian

	switch v := iv.(type) {

	case int:
		{
			err = binary.Write(w, order, int64(v))
			n += uint64Size
			return
		}
	case string:
		{
			err = binary.Write(w, order, int64(len(v)))
			n, err = w.Write([]byte(v))
			n += uint64Size
			return
		}
	case bool:
		{
			var n1 int
			if v {
				n1, err = w.Write([]byte{trueChar})
			} else {
				n1, err = w.Write([]byte{falseChar})
			}
			n += n1
			return
		}
	case *roaring.Bitmap:
		{
			var n64 int64
			n64, err = v.WriteTo(w)
			n = int(n64)
			return
		}
	case interface{}:
		{
			rt := reflect.TypeOf(iv)
			kt := rt.Kind()
			switch kt {
			case reflect.Slice, reflect.Array:
				{
					rv := reflect.ValueOf(iv)
					n = uint64Size
					err = binary.Write(w, order, int64(rv.Len()))
					return
				}
			default:
				err = fmt.Errorf("unhandled interface type occurred: %T", iv)
				return
			}
		}
	}
	err = fmt.Errorf("unhandled type occurred: %T", iv)
	return
}

func (t BinaryCodec) DecodeAsInt(v *int, r io.Reader) (readSize int, err error) {
	const uint64Size int = 8
	var n int
	uintBuff := [uint64Size]byte{}
	n, err = r.Read(uintBuff[:])
	readSize += n
	if err != nil {
		err = errors.Wrap(err, "could not read bytes for int value")
		return
	}
	*v = int(binary.LittleEndian.Uint64(uintBuff[:]))
	return
}

func (t BinaryCodec) DecodeAsBool(v *bool, r io.Reader) (readSize int, err error) {
	const uint64Size int = 8
	var n int
	boolBuff := [1]byte{}
	n, err = r.Read(boolBuff[:])
	readSize += n
	if err != nil {
		err = errors.Wrap(err, "could not read bytes for int value")
		return
	}
	*v = boolBuff[0] == trueChar
	return
}

func (t BinaryCodec) DecodeAsByteSlice(v *[]byte, r io.Reader) (readSize int, err error) {
	var n, l int
	n, err = t.DecodeAsInt(&l, r)
	readSize += n
	if err != nil {
		err = errors.Wrap(err, "reading slice length")
		return
	}
	*v = make([]byte, l)
	n, err = r.Read(*v)
	readSize += n
	if err != nil {
		err = errors.Wrapf(err, "reading slice of [%v]bytes", l)
		return
	}
	return
}

func (t BinaryCodec) DecodeAsString(v *string, r io.Reader) (readSize int, err error) {
	var b *[]byte
	readSize, err = t.DecodeAsByteSlice(b, r)
	defer func() {
		if *b != nil {
			*v = string(*b)
		}
	}()
	if err != nil {
		return
	}
	if *b == nil {
		err = errors.New("getting a string value: slice is null")
		return
	}
	return
}

func (t BinaryCodec) DecodeAsStringSlice(ss *[]string, r io.Reader) (readSize int, err error) {
	var n, l int
	n, err = t.DecodeAsInt(&l, r)
	readSize += n
	if err != nil {
		err = errors.Wrap(err, "reading length of a string slice")
		return
	}
	*ss = make([]string, l)
	for i := range *ss {
		var s string
		n, err = t.DecodeAsString(&s, r)
		readSize += n
		if err != nil {
			err = errors.Wrap(err, "reading value of a string in a slice")
			return
		}
		(*ss)[i] = s
	}
	return
}
