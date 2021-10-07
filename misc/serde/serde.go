package serde

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"reflect"

	"github.com/pkg/errors"
)

const empty byte = 0
const nonEmpty byte = 0xFF
const intSize = 8

var isDebug bool = false

func ByteWriteTo(w io.Writer, payload byte) (total int64, err error) {
	var ni int
	var buffer [1]byte
	if isDebug {
		log.Printf("B-ByteWriteTo %v ", payload)
	}
	buffer[0] = payload
	ni, err = w.Write(buffer[:])
	if err != nil {
		err = errors.Wrap(err, "couldn't serialize byte value")
		return
	}
	if ni != len(buffer) {
		err = errors.Errorf("Written data length %v. Expected %v", ni, len(buffer))
		return
	}
	total += int64(ni)
	if isDebug {
		log.Printf("E-ByteWriteTo")
	}
	return
}
func ByteReadFrom(payload *byte, r io.Reader) (total int64, err error) {
	var ni int
	var buffer [1]byte
	if isDebug {
		log.Printf("B-ByteReadFrom")
	}
	ni, err = r.Read(buffer[:])
	if err != nil {
		err = errors.Wrap(err, "couldn't deserialize byte value")
		return
	}
	if ni != len(buffer) {
		err = errors.Errorf("Read data length %v. Expected %v", ni, len(buffer))
		return
	}
	*payload = buffer[0]
	total += int64(ni)
	if isDebug {
		log.Printf("E-ByteReadFrom %v", *payload)
	}
	return total, nil
}

func IntWriteTo(w io.Writer, payload int64) (total int64, err error) {
	if isDebug {
		log.Printf("B-IntWriteTo %v ", payload)
	}
	uValue := uint64(payload)
	err = binary.Write(w, binary.BigEndian, uValue)
	if err != nil {
		err = errors.Wrap(err, "couldn't serialize integer value")
		return
	}
	total += int64(intSize)
	if isDebug {
		log.Printf("E-IntWriteTo")
	}
	return
}
func IntReadFrom(payload *int64, r io.Reader) (total int64, err error) {
	if isDebug {
		log.Printf("B-IntReadFrom")
	}
	err = binary.Read(r, binary.BigEndian, payload)
	if err != nil {
		err = errors.Wrap(err, "couldn't deserialize integer value")
		return
	}
	total += intSize
	if isDebug {
		log.Printf("E-IntReadFrom %v", *payload)
	}
	return total, nil
}

func StringWriteTo(w io.Writer, payload string) (total int64, err error) {
	var ni int
	var ni64 int64
	if isDebug {
		log.Printf("B-StringWriteTo %v ", payload)
	}
	ni64, err = IntWriteTo(w, int64(len(payload)))
	if err != nil {
		err = errors.Wrap(err, "couldn't serialize string length")
		return
	}
	total += ni64
	buffer := []byte(payload)
	ni, err = w.Write(buffer)
	if err != nil {
		err = errors.Wrap(err, "couldn't serialize string data")
		return
	}
	if ni != len(buffer) {
		err = errors.Errorf("Written data length %v. Expected %v", ni, len(buffer))
		return
	}
	total += int64(ni)
	if isDebug {
		log.Printf("E-StringWriteTo")
	}
	return
}
func StringReadFrom(payload *string, r io.Reader) (total int64, err error) {
	var ni64 int64
	var sLen int64
	if isDebug {
		log.Printf("B-StringReadFrom")
	}
	ni64, err = IntReadFrom(&sLen, r)
	if err != nil {
		err = errors.Wrap(err, "could not read string length")
		return
	}
	total += ni64
	if sLen < 0 {
		err = errors.New("couldn't create string. Got negative length")
		return
	}
	buffer := bytes.Buffer{}
	buffer.Grow(int(sLen))
	ni64, err = io.CopyN(&buffer, r, sLen)
	if err != nil {
		err = errors.Wrapf(err, "couldn't read string data")
		return
	}
	if ni64 != int64(sLen) {
		err = errors.Errorf("Written data length %v. Expected %v", ni64, sLen)
		return
	}
	total += ni64
	*payload = buffer.String()
	if isDebug {
		log.Printf("E-StringReadFrom %v", *payload)
	}
	return
}

func SliceWriteTo(w io.Writer, x interface{}) (total int64, err error) {
	var ni64 int64
	if x == nil {
		if isDebug {
			log.Printf("B-SliceWriteTo nil")
		}
		total, err = IntWriteTo(w, 0)
		if isDebug {
			log.Printf("E-SliceWriteTo")
		}
		return
	}
	xValue := reflect.ValueOf(x)
	xType := xValue.Type()
	xKind := xValue.Kind()

	switch xKind {
	case reflect.Slice, reflect.Array:
		{
			sLen := int64(xValue.Len())
			if isDebug {
				log.Printf("B-SliceWriteTo len=%v", sLen)
			}
			ni64, err = IntWriteTo(w, sLen)
			if err != nil {
				err = errors.Wrap(err, "serializing slice length")
				return ni64, err
			}
			for i := 0; i < int(sLen); i++ {
				xElem := xValue.Index(i)
				xElemKind := xElem.Kind()
				switch xElemKind {
				case reflect.String:
					ni64, err = StringWriteTo(w, xElem.String())
					break
				case reflect.Int, reflect.Uint:
					ni64, err = IntWriteTo(w, int64(xElem.Int()))
					break
				case reflect.Array, reflect.Slice:
					if isDebug {
						log.Printf("B-SliceWriteTo::slice of %v", xElem.Interface())
					}
					ni64, err = SliceWriteTo(w, xElem.Interface())
					if isDebug {
						log.Printf("E-SliceWriteTo::slice of %v", xElem.Interface())
					}
					break
				default:
					if xWriterTo, implemented := xElem.Interface().(io.WriterTo); !implemented {
						err = errors.Errorf("method WriteTo is not implemented for value [%#v] of type %v at position #%v", xElem.Kind().String(), reflect.TypeOf(xElem), i)
						return 0, err
					} else {
						if isDebug {
							log.Printf("B-SliceWriteTo::WriteTo %v", xWriterTo)
						}
						ni64, err = xWriterTo.WriteTo(w)
						if err != nil {
							err = errors.Wrapf(err, "could not serialize a slice value [%#v] at position [%v]", xElem.Kind().String(), i)
						}
						total += ni64
						if isDebug {
							log.Printf("E-SliceWriteTo::WriteTo %v]", xWriterTo)
						}
					}
				}
			}
		}
	default:
		err = errors.Errorf("given value neither slice nor array: %v", xType)
		return

	}
	if isDebug {
		log.Printf("E-SliceWriteTo")
	}
	return
}

func SliceReadFrom(fMake func(sCap int) interface{}, r io.Reader) (total int64, err error) {
	var ni64 int64
	var sCap int64
	if isDebug {
		log.Printf("B-SliceReadFrom")
	}
	ni64, err = IntReadFrom(&sCap, r)
	if err != nil {
		err = errors.Wrap(err, "could not read slice length")
		return
	}
	total += ni64
	if sCap == 0 {
		if isDebug {
			log.Printf("E-SliceReadFrom nil")
		}
		return
	}

	if sCap < 0 {
		err = errors.New("couldn't create slice. Got negative length")
		return
	}

	x := fMake(int(sCap))
	xSlice := reflect.ValueOf(x)
	xType := xSlice.Type()
	xKind := xType.Kind()

	switch xKind {

	case reflect.Slice, reflect.Array:
		{
			for i := 0; i < int(sCap); i++ {
				xValue := xSlice.Index(i)
				xValueType := xValue.Type()
				xValueKind := xValue.Kind()
				switch xValueKind {
				case reflect.String:
					var xs *string
					ni64, err = StringReadFrom(xs, r)
					if err != nil {
						err = errors.Errorf("couldn't read string value at position #%v", i)
						return ni64, err
					}
					xValue.Set(reflect.ValueOf(*xs))
					total += ni64
					break
				case reflect.Int, reflect.Uint:
					var xi int64
					ni64, err = IntReadFrom(&xi, r)
					if err != nil {
						err = errors.Errorf("couldn't read int value at position #%v", i)
						return ni64, err
					}
					xValue.Set(reflect.ValueOf(int64(xi)))
					total += ni64
					break
				case reflect.Array, reflect.Slice:
					xfMake := func(sCap int) interface{} {
						xSubSlice := reflect.MakeSlice(xValueType, sCap, sCap)
						xValue.Set(xSubSlice)
						return xSubSlice.Interface()
					}
					if isDebug {
						log.Printf("B-SliceReadFrom::slice of %v", xValueType)
					}
					ni64, err = SliceReadFrom(xfMake, r)
					if isDebug {
						log.Printf("E-SliceReadFrom::slice of %v", xValueType)
					}
					break
				default:
					xValuePointer := xValue.Addr()
					xValueKind := xValue.Kind()
					if xReaderFrom, implemented := xValuePointer.Interface().(io.ReaderFrom); !implemented {
						err = errors.Errorf("method ReadFrom is not implemented for reference [%T] at position #%v", xValuePointer.Elem().Type().Name(), i)
						return ni64, err
					} else {
						if isDebug {
							log.Printf("B-SliceReadFrom::ReadFrom %v", xReaderFrom)
						}
						ni64, err = xReaderFrom.ReadFrom(r)
						if err != nil {
							err = errors.Wrapf(err, "could not deserialize a slice value [%#v] at position [%v]", xValueKind.String(), i)
							return ni64, err
						}
						// fill capacity is allocated. Slice pointer must remain same
						/*if xValueKind == reflect.Struct {
							xValue.Set(xValuePointer.Elem())
						} else {
							xValue.Set(xValuePointer)
						}*/
						total += ni64
						if isDebug {
							log.Printf("E-SliceReadFrom::ReadFrom %v", xReaderFrom)
						}
					}
				}
			}
		}
	default:
		err = errors.Errorf("the made value neither slice nor array: %v", xType)
		return
	}
	if isDebug {
		log.Printf("E-SliceReadFrom")
	}
	return
}
