package iix

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"

	"github.com/pkg/errors"

	"github.com/RoaringBitmap/roaring"
)

const (
	BASE_SEQ_FLAG byte = 'B'
	LAKE_SEQ_FLAG byte = 'L'
)

type StringSliceIterator interface {
	Next() (string, bool)
}

type JointColumnProps struct {
	seqId    int
	ColumnID string
}

type JointColumnPropSlice []*JointColumnProps

func (slice JointColumnPropSlice) ColumnIDs() []string {
	result := make([]string, 0, len(slice))
	for i := range slice {
		result = append(result, slice[i].ColumnID)
	}
	return result
}

type JointColumnPropMap map[int]*JointColumnProps

func (m JointColumnPropMap) ColumnIDs() []string {
	result := make([]string, 0, len(m))
	for _, props := range m {
		result = append(result, props.ColumnID)
	}
	return result
}

type NodeKeyer interface {
	BaseKey() string
	LakeKey() string
	Key() string
}
type NodeKeyerMap interface {
	ForEach(f func(string, NodeKeyer) bool) bool
	Len() int
}

type ColumnJoint struct {
	baseKey       string
	lakeKey       string
	LakeColumns   JointColumnPropMap
	BaseColumns   JointColumnPropMap
	uniqueValueBs *roaring.Bitmap
	//suppressed    bool
}
type ColumnJointSlice []*ColumnJoint
type ColumnJointMap map[string]*ColumnJoint

type ColumnGroup struct {
	ID               string
	JointMap         ColumnJointMap     `json:"ColumnJoints"`
	BlankLakeColumns JointColumnPropMap //JointColumnPropSlice
	//BlankBaseColumns map[string]bool
	baseGroupKey string
	lakeGroupKey string
	lakeNumRowBs *roaring.Bitmap
	baseNumRowBs *roaring.Bitmap
}

type ColumnGroupSlice []*ColumnGroup
type ColumnGroupMap map[string]*ColumnGroup

func (gss ColumnGroupSlice) PrintResult(toColumnNames func(string, []string) ([]string, error)) {
	for _, gs := range gss {
		gs.PrintResult(toColumnNames)
	}
}

func (m ColumnGroupMap) PrintResult(toColumnNames func(string, []string) ([]string, error)) {

	/*
	   toColumnNames := func(metaSide string, ids []string) (names []string, err error) {
	   		names = make([]string, len(ids))
	   		copy(names, ids)
	   		var found *meta.Column
	   		for i := range ids {
	   			if metaSide == "L" {
	   				found, err = h.lakeMeta.ColumnById(ctx, ids[i])
	   			} else {
	   				found, err = h.baseMeta.ColumnById(ctx, ids[i])
	   			}
	   			names = append(names, found.Name)
	   		}

	   		return
	   	}
	   	_ = toColumnNames

	   	gss.PrintResult(meta.ColumnNamesFromIDs)
	*/

	for _, gs := range m {
		gs.PrintResult(toColumnNames)
	}
}

func (cg ColumnGroup) PrintResult(toColumnNames func(string, []string) ([]string, error)) {
	//uq := float64(0)
	joints := make(ColumnJointSlice, 0, len(cg.JointMap))
	var buq *roaring.Bitmap
	for _, joint := range cg.JointMap {
		// if joint.suppressed {
		// 	continue
		// }
		if buq == nil {
			buq = joint.uniqueValueBs.Clone()
		}
		buq.Or(joint.uniqueValueBs)
		joints = append(joints, joint)
	}
	sort.Slice(joints, func(i, j int) bool {
		return joints[i].Key() < joints[j].Key()
	})

	var uqb, uql float64
	if cg.lakeNumRowBs != nil {
		uql = float64(cg.lakeNumRowBs.GetCardinality())
	}
	if cg.baseNumRowBs != nil {
		uqb = float64(cg.baseNumRowBs.GetCardinality())
	}
	fmt.Printf("%6v %6.0f|%6.0f|%.3f", cg.ID, uql, uqb, float64(buq.GetCardinality())/uqb)

	for j := range joints {
		var colsL, colsB []string
		colsL, _ = toColumnNames("L", joints[j].LakeColumns.ColumnIDs())
		card := 0
		if joints[j].uniqueValueBs != nil {
			card = int(joints[j].uniqueValueBs.GetCardinality())
		}
		colsB, _ = toColumnNames("B", joints[j].BaseColumns.ColumnIDs())
		// for z := range colsB {
		// 	if cg.Joints[j].BaseColumns[z].BlankRnBs != nil {
		// 		card := cg.Joints[j].BaseColumns[z].BlankRnBs.GetCardinality()
		// 		if card > 0 {
		// 			colsB[z] = fmt.Sprintf("%v*(%v, %.2f %%)*", colsB[z], card, float64(card*100)/float64(brc))
		// 		}
		// 	}
		// }
		sort.Strings(colsL)
		sort.Strings(colsB)
		_ = card
		fmt.Printf("[%v/%v/%v]",
			strings.Join(colsL, ","),
			card,
			strings.Join(colsB, ","),
		)
		//fmt.Printf(" %v ", joints[j].Key())
	}
	for _, props := range cg.BlankLakeColumns {
		fmt.Printf("{%v}", props.ColumnID)
	}
	/*for columnID, _ := range cg.BlankBaseColumns {
		fmt.Print("{")
		fmt.Print(columnID)
		fmt.Print("}")
	}*/

	fmt.Println()
}

func (cg ColumnGroup) PrintQuery(toColumnNames func(string, []string) ([]string, error)) {
	//uq := float64(0)
	joints := make(ColumnJointSlice, 0, len(cg.JointMap))
	for _, joint := range cg.JointMap {
		// if joint.suppressed {
		// 	continue
		// }
		joints = append(joints, joint)
	}

	for j := range joints {
		var colsL, colsB []string
		colsL, _ = toColumnNames("L", joints[j].LakeColumns.ColumnIDs())
		colsB, _ = toColumnNames("B", joints[j].BaseColumns.ColumnIDs())
		sort.Strings(colsL)
		sort.Strings(colsB)
		for _, cb := range colsB {
			for _, cl := range colsL {
				fmt.Printf(" and (%v = %v or (%v is null and  %v is null))\n", cl, cb, cl, cb)
			}
		}
	}

	fmt.Println()
}

type iMapper interface {
	mapBaseColumn(string) (int, bool)
	mapLakeColumn(string) (int, bool)
	baseColumnMapped(string) (int, bool)
	lakeColumnMapped(string) (int, bool)
}

/*
func (sub *ColumnGroup) IsSubSetOf(super *ColumnGroup, mapper iMapper) jointRelationSlice {
	return nil
}*/

func (j ColumnJoint) Key() string {
	return j.baseKey + j.lakeKey
}
func (j ColumnJoint) BaseKey() string {
	return j.baseKey
}
func (j ColumnJoint) LakeKey() string {
	return j.lakeKey
}

func (j ColumnJoint) CreateJointKeys() (baseKey, lakeKey string) {
	lsq := newCharSeq()
	bsq := newCharSeq()
	for seqId := range j.BaseColumns {
		bsq.Add(seqId)
	}
	for seqId := range j.LakeColumns {
		lsq.Add(seqId)
	}
	return bsq.String(BASE_SEQ_FLAG), lsq.String(LAKE_SEQ_FLAG)
}

func (m ColumnJointMap) ForEach(f func(string, NodeKeyer) bool) bool {
	for k, nk := range m {
		// if nk.suppressed {
		// 	continue
		// }
		if quit := f(k, nk); quit {
			return quit
		}
	}
	return false
}
func (m ColumnJointMap) Len() int {
	return len(m)
	// c := 0
	// for _, nk := range m {
	// 	 if nk.suppressed {
	// 	 	continue
	// 	 }
	// 	c++
	// }
	// return c
}

func (m ColumnJointMap) CreateGroupKeys() (baseKey, lakeKey string) {
	lsq := newCharSeq()
	bsq := newCharSeq()
	for _, joint := range m {
		for seqId := range joint.BaseColumns {
			bsq.Add(seqId)
		}
		for seqId := range joint.LakeColumns {
			lsq.Add(seqId)
		}
	}
	return bsq.String(BASE_SEQ_FLAG), lsq.String(LAKE_SEQ_FLAG)
}

func (m ColumnJointMap) GroupKey() string {
	pairs := make([]string, 0, len(m))
	for j := range m {
		pairs = append(pairs, m[j].baseKey+m[j].lakeKey)
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i] < pairs[j]
	})
	groupKey := strings.Join(pairs, ".")
	return groupKey
}

func (g ColumnGroup) WriteTo(w io.Writer) (writtenBytes int, err error) {
	const uint64Size int = 8
	type strategyFunc = func(interface{}) (int, error)

	bufferWriter := func(iv interface{}) (n int, err error) {
		var order = binary.LittleEndian

		switch v := iv.(type) {

		case int:
			{
				n = uint64Size
				err = binary.Write(w, order, int64(v))
				return
			}
		case string:
			{
				err = binary.Write(w, order, int64(len(v)))
				n, err = w.Write([]byte(v))
				n += uint64Size
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

	traverse := func(sf strategyFunc) (total int, err error) {
		var n int
		n, err = sf(g.ID)
		if err != nil {
			err = errors.Errorf("could not process g.id. %v", err)
			return
		}

		total += n
		n, err = sf(g.lakeGroupKey)
		if err != nil {
			err = errors.Errorf("could not process g.LakeKey. %v", err)
			return
		}
		total += n

		n, err = sf(g.baseGroupKey)
		if err != nil {
			err = errors.Errorf("could not process g.BaseKey. %v", err)
			return
		}
		total += n

		n, err = sf(g.lakeNumRowBs)
		if err != nil {
			err = errors.Wrapf(err, "could not process g.lakeNumRowBs)")
			return
		}
		total += n
		n, err = sf(g.baseNumRowBs)
		if err != nil {
			err = errors.Wrapf(err, "could not process g.baseNumRowBs")
			return
		}
		total += n
		n, err = sf(len(g.JointMap))
		if err != nil {
			err = errors.Wrapf(err, "could not process g.Joints length")
			return
		}
		total += n

		for jkey, joint := range g.JointMap {
			n, err = sf(jkey)
			if err != nil {
				err = errors.Wrapf(err, "could not process jkey")
				return
			}
			total += n

			n, err = sf(joint.lakeKey)
			if err != nil {
				err = errors.Wrapf(err, "could not process lakeKey")
				return
			}
			total += n
			n, err = sf(joint.baseKey)
			if err != nil {
				err = errors.Wrapf(err, "could not process baseKey")
				return
			}
			total += n

			n, err = sf(len(joint.LakeColumns))
			if err != nil {
				err = errors.Wrapf(err, "could not process LakeColumnIDs length")
				return
			}
			total += n
			for _, props := range joint.LakeColumns {
				n, err = sf(props.ColumnID)
				if err != nil {
					err = errors.Wrapf(err, "could not process LakeColumnID")
					return
				}
				total += n
				n, err = sf(props.seqId)
				if err != nil {
					err = errors.Wrapf(err, "could not process LakeColumn seqId")
					return
				}
				total += n
			}

			n, err = sf(len(joint.BaseColumns))
			if err != nil {
				err = errors.Wrapf(err, "could not process BaseColumnIDs length")
				return
			}
			total += n
			for _, props := range joint.BaseColumns {
				n, err = sf(props.ColumnID)
				if err != nil {
					err = errors.Wrapf(err, "could not process BaseColumnID")
					return
				}
				total += n

				n, err = sf(props.seqId)
				if err != nil {
					err = errors.Wrapf(err, "could not process BaseColumnID seqId")
					return
				}
				total += n

			}

			n, err = sf(joint.uniqueValueBs)
			if err != nil {
				err = errors.Wrapf(err, "could not process uniqueValueBs")
				return
			}
			total += n
		}

		n, err = sf(len(g.BlankLakeColumns))
		if err != nil {
			err = errors.Wrapf(err, "could not process g.BlankLakeColumns length")
			return
		}
		for _, props := range g.BlankLakeColumns {
			n, err = sf(props.ColumnID)
			if err != nil {
				err = errors.Wrapf(err, "could not process BlankLakeColumns.ColumnID")
				return
			}
		}

		return total, nil
	}

	writtenBytes, err = traverse(bufferWriter)
	if err != nil {
		return -1, err
	}
	return
}
func (g *ColumnGroup) ReadFrom(r io.Reader) (readSize int, err error) {
	nextInt := func() (v int, readSize int, err error) {
		const uint64Size int = 8
		var n int
		uintBuff := [uint64Size]byte{}
		n, err = r.Read(uintBuff[:])
		readSize += n
		if err != nil {
			err = errors.Wrap(err, "could not read bytes for int value")
			return
		}
		v = int(binary.LittleEndian.Uint64(uintBuff[:]))
		return
	}

	nextByteSlice := func() (v []byte, readSize int, err error) {
		var n, l int
		l, n, err = nextInt()
		readSize += n
		if err != nil {
			err = errors.Wrap(err, "reading byte slice length")
			return
		}
		v = make([]byte, l)
		n, err = r.Read(v)
		readSize += n
		if err != nil {
			err = errors.Wrapf(err, "reading slice of [%v]bytes", l)
			return
		}
		return
	}

	nextString := func() (v string, readSize int, err error) {
		var b []byte
		b, readSize, err = nextByteSlice()
		defer func() {
			if b != nil {
				v = string(b)
			}
		}()

		if err != nil {
			return
		}
		if b == nil {
			err = errors.New("getting a string value: slice is null")
			return
		}
		return
	}

	var n, l int

	g.ID, n, err = nextString()
	readSize += n
	if err != nil {
		err = errors.Wrap(err, "reading id")
		return
	}
	g.lakeGroupKey, n, err = nextString()
	readSize += n
	if err != nil {
		err = errors.Wrap(err, "reading LakeKey")
		return
	}

	g.baseGroupKey, n, err = nextString()
	readSize += n
	if err != nil {
		err = errors.Wrap(err, "reading BaseKey")
		return
	}

	/*
		l, n, err = nextInt()
		if err != nil {
			err = errors.Wrap(err, "reading if Blank columns there: %w")
			return
		}

			if l != 0 {
				g.Blank = new(ColumnJointStats)
				g.Blank.LakeColumnIDs, n, err = nextStringSlice()
				readSize += n
				if err != nil {
					err = errors.Wrap(err, "reading LakeColumnIDs")
					return
				}
				g.Blank.BaseColumnIDs, n, err = nextStringSlice()
				readSize += n
				if err != nil {
					err = errors.Wrap(err, "reading BaseColumnIDs")
					return
				}
			}*/
	{
		g.lakeNumRowBs = roaring.NewBitmap()
		var n64 int64
		n64, err = g.lakeNumRowBs.ReadFrom(r)
		readSize += int(n64)
		if err != nil {
			err = errors.Wrapf(err, "could not process g.lakeNumRowBs)")
			return
		}
		g.baseNumRowBs = roaring.NewBitmap()
		n64, err = g.baseNumRowBs.ReadFrom(r)
		readSize += int(n64)
		if err != nil {
			err = errors.Wrapf(err, "could not process g.baseNumRowBs)")
			return
		}
	}

	jl, n, err := nextInt()
	readSize += n
	if err != nil {
		err = errors.Wrap(err, "reading length of Joints: %w")
		return
	}

	g.JointMap = make(ColumnJointMap, jl)
	for j := 0; j < jl; j++ {
		joint := new(ColumnJoint)
		var jKey string
		jKey, n, err = nextString()
		if err != nil {
			err = errors.Wrap(err, "reading joint key")
			return
		}

		joint.lakeKey, n, err = nextString()
		if err != nil {
			err = errors.Wrap(err, "reading lakeKey key")
			return
		}
		readSize += n

		joint.baseKey, n, err = nextString()
		if err != nil {
			err = errors.Wrap(err, "reading baseKey key")
			return
		}
		readSize += n

		l, n, err = nextInt()
		if err != nil {
			err = errors.Wrap(err, "reading Lake columns length")
			return
		}
		readSize += n

		joint.LakeColumns = make(JointColumnPropMap)
		for z := 0; z < l; z++ {
			//*******same is for Base part
			cp := new(JointColumnProps)
			cp.ColumnID, n, err = nextString()
			if err != nil {
				err = errors.Wrap(err, "reading Lake column ID")
				return
			}
			readSize += n

			cp.seqId, n, err = nextInt()
			if err != nil {
				err = errors.Wrap(err, "reading Lake column seq ID")
				return
			}
			readSize += n
			//*******same is for Base part
			joint.LakeColumns[cp.seqId] = cp
		}

		l, n, err = nextInt()
		if err != nil {
			err = errors.Wrap(err, "reading Base columns length")
			return
		}

		readSize += n
		joint.BaseColumns = make(JointColumnPropMap)
		for z := 0; z < l; z++ {
			cp := new(JointColumnProps)
			cp.ColumnID, n, err = nextString()
			if err != nil {
				err = errors.Wrap(err, "reading Base column ID")
				return
			}
			readSize += n

			cp.seqId, n, err = nextInt()
			if err != nil {
				err = errors.Wrap(err, "reading Base column seq ID")
				return
			}
			readSize += n

			joint.BaseColumns[cp.seqId] = cp
		}
		joint.uniqueValueBs = roaring.NewBitmap()
		var n64 int64
		n64, err = joint.uniqueValueBs.ReadFrom(r)
		readSize += int(n64)
		if err != nil {
			err = errors.Wrap(err, "getting unique value bitset state from buffer")
			return
		}
		g.JointMap[jKey] = joint
	}
	l, n, err = nextInt()
	if l > 0 {
		g.BlankLakeColumns = make(JointColumnPropMap)
		for z := 0; z < l; z++ {
			cp := &JointColumnProps{}
			cp.ColumnID, n, err = nextString()
			if err != nil {
				err = errors.Wrap(err, "getting BLANK lake column name")
				return
			}
			readSize += n
			g.BlankLakeColumns[cp.seqId] = cp
		}
	}

	return
}
