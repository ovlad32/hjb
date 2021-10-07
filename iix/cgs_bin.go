package iix

/*
import (
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/ovlad32/hjb/iix/internal/utils"
	"github.com/pkg/errors"
)

type cgBinConvert struct {
	version      int
	magic        string
	closingTag   string
	columnGroups *ColumnGroups
	bin          *utils.BinaryCodec
}

func (t cgBinConvert) WriteTo(w io.Writer) (writtenBytes int, err error) {
	var n int
	n, err = t.bin.EncodeFrom(w, t.magic)
	if err != nil {
		err = errors.Wrap(err, "could not seralize the MagicWord")
		return
	}

	n, err = t.bin.EncodeFrom(w, t.version)
	if err != nil {
		err = errors.Wrap(err, "could not seralize the version")
		return
	}
	n, err = t.bin.EncodeFrom(w, len(*t.columnGroups))
	if err != nil {
		err = errors.Wrap(err, "could not seralize the number of column groups")
		return
	}

	for _, cg := range *t.columnGroups {
		n, err = t.bin.EncodeFrom(w, cg.ID)
		if err != nil {
			err = errors.Wrap(err, "could not seralize ID")
			return
		}

		writtenBytes += n
		n, err = t.bin.EncodeFrom(w, cg.LakeRowCount)
		if err != nil {
			err = errors.Wrap(err, "could not serialize LakeRowCount")
			return
		}
		writtenBytes += n

		n, err = t.bin.EncodeFrom(w, cg.BaseRowCount)
		if err != nil {
			errors.Wrap(err, "could not serialize BaseRowCount")
			return
		}
		writtenBytes += n

		n, err = t.bin.EncodeFrom(w, cg.DistinctRowCount)
		if err != nil {
			errors.Wrap(err, "could not serizalize DistinctRowCount")
			return
		}
		writtenBytes += n

		n, err = t.bin.EncodeFrom(w, cg.LakeAggRowCount)
		if err != nil {
			errors.Wrap(err, "could not serizalize LakeAggRowCount")
			return
		}
		writtenBytes += n

		n, err = t.bin.EncodeFrom(w, cg.BaseAggRowCount)
		if err != nil {
			errors.Wrap(err, "could not serizalize BaseAggRowCount")
			return
		}
		writtenBytes += n

		n, err = t.bin.EncodeFrom(w, cg.numRowsBs != nil)
		if err != nil {
			errors.Wrap(err, "could not serizalize numRowsBs existance flag")
			return
		}
		writtenBytes += n

		if cg.numRowsBs != nil {
			n, err = t.bin.EncodeFrom(w, cg.numRowsBs)
			if err != nil {
				err = errors.Wrap(err, "could not serizalize the distinct row bitset state")
				return
			}
			writtenBytes += n
		}

		n, err = t.bin.EncodeFrom(w, cg.Joints)
		if err != nil {
			err = errors.Wrap(err, "could not serizalize Joints lengh")
			return
		}
		writtenBytes += n

		for i := range cg.Joints {
			n, err = t.bin.EncodeFrom(w, cg.Joints[i].LakeColumnIDs)
			if err != nil {
				err = errors.Wrap(err, "could not serizalize LakeColumnIDs length")
				return
			}
			writtenBytes += n
			for j := range cg.Joints[i].LakeColumnIDs {
				n, err = t.bin.EncodeFrom(w, cg.Joints[i].LakeColumnIDs[j])
				if err != nil {
					err = errors.Wrap(err, "could not serizalize a SwampColumnName")
					return
				}
				writtenBytes += n
			}
			n, err = t.bin.EncodeFrom(w, cg.Joints[i].BaseColumnIDs)
			if err != nil {
				err = errors.Wrap(err, "could not serizalize BaseColumnIDs length")
				return
			}
			writtenBytes += n

			for j := range cg.Joints[i].BaseColumnIDs {
				n, err = t.bin.EncodeFrom(w, cg.Joints[i].BaseColumnIDs[j])
				if err != nil {
					err = errors.Wrap(err, "could not serizalize a BaseColumnID")
					return
				}
				writtenBytes += n
			}

			n, err = t.bin.EncodeFrom(w, cg.Joints[i].uniqueValueBs != nil)
			if err != nil {
				errors.Wrap(err, "could not serizalize numRowsBs existance flag")
				return
			}
			writtenBytes += n

			if cg.Joints[i].uniqueValueBs != nil {
				n, err = t.bin.EncodeFrom(w, cg.Joints[i].uniqueValueBs)
				if err != nil {
					err = errors.Wrap(err, "could not serizalize Joint bitset")
					return
				}
				writtenBytes += n
			}

			n, err = t.bin.EncodeFrom(w, t.closingTag)
			if err != nil {
				err = errors.Wrap(err, "could not serizalize record closing tag")
				return
			}
		}
	}
	return
}

func (t cgBinConvert) ReadFrom(r io.Reader) (readSize int, err error) {
	var n, l int
	var n64 int64
	var s string
	suppressEOF := func(err error) error {
		if err == io.EOF {
			err = errors.New("ColumnGroup record is incomplete due to EOF encountered")
		}
		return err
	}

	n, err = t.bin.DecodeAsString(&s, r)
	if err != nil {
		err = errors.Wrap(err, "could not read leading data")
	}
	if t.magic != s {
		err = errors.New("Stream doesn't have the proper leading data sequence")
		return
	}
	n, err = t.bin.DecodeAsInt(&l, r)
	if err != nil {
		err = errors.Wrap(err, "could not read version")
		return
	}
	if l != 1 {
		err = errors.New("Stream doesn't have the proper version")
		return
	}
	n, err = t.bin.DecodeAsInt(&l, r)
	if err != nil {
		err = errors.Wrap(err, "could not the number of records")
		return
	}
	if l < 0 {
		err = errors.New("Stream doesn't have the proper number of records")
		return
	}

	lcgs := make(ColumnGroups, 0, l)

	for recIndex := 0; recIndex < cap(lcgs); recIndex++ {
		var cg ColumnGroupStats

		n, err = t.bin.DecodeAsString(&cg.ID, r)
		readSize += n
		if err != nil {
			err = errors.Wrap(suppressEOF(err), "could not read ID")
			return
		}

		n, err = t.bin.DecodeAsInt(&cg.LakeRowCount, r)
		readSize += n
		if err != nil {
			err = errors.Wrap(suppressEOF(err), "could not read LakeRowCount")
			return
		}

		n, err = t.bin.DecodeAsInt(&cg.BaseAggRowCount, r)
		readSize += n
		if err != nil {
			err = errors.Wrap(suppressEOF(err), "could not read BaseAggRowCount")
			return
		}

		n, err = t.bin.DecodeAsInt(&cg.DistinctRowCount, r)
		readSize += n
		if err != nil {
			err = errors.Wrap(suppressEOF(err), "could not read DistinctRowCount")
			return
		}

		n, err = t.bin.DecodeAsInt(&l, r)
		readSize += n
		if err != nil {
			err = errors.Wrap(suppressEOF(err), "could not read length of Joints")
			return
		}

		cg.Joints = make(ColumnJoints, l)
		for i := range cg.Joints {
			n, err = t.bin.DecodeAsStringSlice(&cg.Joints[i].LakeColumnIDs, r)
			readSize += n
			if err != nil {
				err = errors.Wrap(suppressEOF(err), "could not read LakeColumnIDs for a Joint")
				return
			}
			n, err = t.bin.DecodeAsStringSlice(&cg.Joints[i].BaseColumnIDs, r)
			readSize += n
			if err != nil {
				err = errors.Wrap(suppressEOF(err), "could not read BaseColumnIDs for a Joint")
				return
			}
			cg.Joints[i].uniqueValueBs = roaring.NewBitmap()
			n64, err = cg.Joints[i].uniqueValueBs.ReadFrom(r)
			readSize += int(n64)
			if err != nil {
				err = errors.Wrap(suppressEOF(err), "could not read the value bitset state")
				return
			}
		}
		n, err = t.bin.DecodeAsString(&s, r)
		readSize += n
		if err != nil {
			// EOF is possibe
			err = errors.Wrap(err, "could not read closing tag")
			return
		}
		if t.closingTag != s {
			err = errors.Errorf("Record is not finished with proper closing tag")
			return
		}
		lcgs = append(lcgs, &cg)
	}
	*t.columnGroups = lcgs
	return
}
*/
