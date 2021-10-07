package iix

import (
	"math"
	"sort"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/ovlad32/hjb/iixs"
)

/*
type rKeyer interface {
	SliceKey(int, func(int) string) string
}
*/
type rowTrack struct {
	curRow        int
	seqId         int
	columnID      string
	nodeIndex     int
	mode          byte
	rnStorage     iixs.IRnStorage
	rnCardinality uint
	blankBitset   *roaring.Bitmap
}

type rowTracks []*rowTrack

/*type blankColumnPair struct {
	baseColumnSeqID int
	lakeColumnSeqID int
}
*/
type iCoverage struct {
	discoveredTrackIndexes []int
	rowNumberBs            *roaring.Bitmap
}

func (ts rowTrack) String() string {
	return ts.columnID
}

func (ts rowTracks) mins() (min1 int, min2 int) {
	if len(ts) == 0 {
		return
	}
	min1 = math.MaxInt64
	min2 = math.MaxInt64
	for i := range ts {
		if ts[i].curRow <= 0 {
			continue
		}
		if min1 > ts[i].curRow || min1 == math.MaxInt64 {
			min2 = min1
			min1 = ts[i].curRow
		} else if min1 < ts[i].curRow && (min2 > ts[i].curRow || min2 == math.MaxInt64) {
			min2 = ts[i].curRow
		}
	}
	return
}

func (ts rowTracks) key(rm rKeyer) (key string, n int) {
	tsc := ts.ColumnIDs()
	sort.Strings(tsc)
	return strings.Join(tsc, "~"), 0
}

func (ts rowTracks) ColumnIDs() []string {
	cs := make([]string, 0, len(ts))
	for i := range ts {
		cs = append(cs, ts[i].columnID)
	}
	return cs
}

func (ts rowTracks) BlankBs() []*roaring.Bitmap {
	cs := make([]*roaring.Bitmap, 0, len(ts))
	for i := range ts {
		cs = append(cs, ts[i].blankBitset)
	}
	return cs
}
