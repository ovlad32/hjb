package iix

import (
	"context"
	"hash/fnv"
	"io"
	"math"
	"sort"
	"sync"

	"github.com/pkg/errors"

	"github.com/RoaringBitmap/roaring"
	"github.com/ovlad32/hjb/iixs"
	"github.com/ovlad32/hjb/meta"
	"github.com/ovlad32/hjb/misc/serde"
)

type DiscoveryingRowStrategy struct {
	initOnce            sync.Once
	lakeMeta            iMetaProvider
	norm                iNormalizer
	baseMeta            iMetaProvider
	finder              IFinder
	anchorUniqueness    float64
	anchorMinCount      int
	anchorMaxCount      int
	baseRowCount        int
	cgsMap              ColumnGroupMap
	seqMapper           iColumnSeqMapper
	seqProvider         iSeqProvider
	columnEmptyCountMap map[string]int64
}

type rKeyer interface {
	keySlice(int, func(int) string) string
}

func NewDiscoveryingRowStrategy(cgsMap ColumnGroupMap, lakeMeta iMetaProvider, norm iNormalizer,
	baseMeta iMetaProvider, ifinder IFinder,
	anchorUniqueness float64, anchorMinCount int, anchorMaxCount int) *DiscoveryingRowStrategy {
	r := &DiscoveryingRowStrategy{
		lakeMeta:            lakeMeta,
		cgsMap:              cgsMap,
		norm:                norm,
		baseMeta:            baseMeta,
		finder:              ifinder,
		anchorUniqueness:    anchorUniqueness,
		anchorMinCount:      anchorMinCount,
		anchorMaxCount:      anchorMaxCount,
		columnEmptyCountMap: make(map[string]int64),
	}
	return r
}
func (h *DiscoveryingRowStrategy) internalInit(ctx context.Context) (err error) {
	h.baseRowCount, err = h.baseMeta.TotalRowCount(ctx)
	h.seqProvider = new(countHolder)
	h.seqMapper = newColumnSeqStorage(h.seqProvider)
	return
}

func (h *DiscoveryingRowStrategy) Handle(ctx context.Context, rowNumber int, originalValues []string) (err error) {

	h.initOnce.Do(func() { err = h.internalInit(ctx) })
	if err != nil {
		err = errors.WithStack(err)
		return
	}

	columns, values, rejectedColumns, err := resolveExtentsFromValues(ctx, h.lakeMeta, h.norm, originalValues)
	_ = rejectedColumns
	if err != nil {
		err = errors.WithStack(err)
		return
	}

	valueToColumnsMap, err := groupByValue(ctx, columns, values)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	if len(valueToColumnsMap) < h.anchorMinCount {
		return
	}
	nodes := make([]valueNode, 0, len(valueToColumnsMap))
	trackCount := 0
	var currentRowBlankLakeColumns meta.Columns
	{
		for value, groupedColumns := range valueToColumnsMap {
			if value == "" {
				currentRowBlankLakeColumns = groupedColumns
				continue
			}
			rcs, found, err := h.finder.Find(value)
			if err != nil {
				err = errors.WithStack(err)
				return err
			}
			if found {
				sort.Slice(groupedColumns, groupedColumns.ByID)
				nodes = append(nodes, valueNode{
					lakeColumns: groupedColumns,
					value:       value,
					rcs:         rcs,
				})
				trackCount += len(groupedColumns)
			}
		}
	}

	// TODO: STILL NEEDED?
	/*sort.Slice(nodes, func(i, j int) bool {
		if len(nodes[i].lakeColumns) == len(nodes[j].lakeColumns) {
			for cIndex := range nodes[i].lakeColumns {
				if nodes[i].lakeColumns[cIndex].Position < nodes[j].lakeColumns[cIndex].Position {
					return true
				}
			}
		} else {
			return len(nodes[i].lakeColumns) < len(nodes[j].lakeColumns)
		}
		return false
	})
	*/

	baseTableTracks := make(rowTracks, 0, trackCount)
	var anchors = 0
	for nodeIndex := range nodes {
		for i, columnID := range nodes[nodeIndex].rcs.ColumnIDs() {
			rnStorage := nodes[nodeIndex].rcs.RowStorage(i)
			valueRowCount := rnStorage.Cardinality()
			emptyCount, found := h.columnEmptyCountMap[columnID]
			if !found {
				var baseColumn *meta.Column
				baseColumn, err = h.baseMeta.ColumnById(ctx, columnID)
				emptyCount = baseColumn.EmptyCount.Int64
				h.columnEmptyCountMap[columnID] = emptyCount
			}
			if err != nil {
				err = errors.WithStack(err)
				return
			}
			rt := &rowTrack{
				columnID:      columnID,
				nodeIndex:     nodeIndex,
				rnStorage:     rnStorage,
				rnCardinality: valueRowCount,
				mode:          'F',
			}
			nonEmptyRowCount := int64(h.baseRowCount) - emptyCount
			columnFullness := float64(valueRowCount) * 100 / float64(nonEmptyRowCount)
			if columnFullness < h.anchorUniqueness {
				anchors++
				rt.mode = 'I'
			}
			baseTableTracks = append(baseTableTracks, rt)
		}
	}
	if anchors < h.anchorMinCount {
		return
	}

	if anchors > h.anchorMaxCount {
		sort.Slice(baseTableTracks, func(i, j int) bool {
			return baseTableTracks[i].rnCardinality < baseTableTracks[j].rnCardinality
		})

		for i := h.anchorMaxCount; i < len(baseTableTracks) && anchors > h.anchorMaxCount; i++ {
			if baseTableTracks[i].rnCardinality < uint(h.baseRowCount/1000) {
				continue
			}
			baseTableTracks[i].mode = 'F'
			anchors--
		}
	}

	/*if followings == len(tracks) {
		ratios := make(map[string]float32, len(tracks))
		for i := range tracks {
			ratios[tracks[i].lookupValue] = float32(tracks[i].rnStorage.Cardinality()) / baseRowCount
		}
		log.Printf("no suitable cardinalities for base column combination: ")
		return
	}*/

	hsh := fnv.New32()
	icvgMap, erre := h.collectIntersections(ctx, baseTableTracks)
	if erre != nil {
		err = errors.WithStack(err)
		return
	}
	if len(icvgMap) == 0 {
		return
	}
	//fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	for _, cvg := range icvgMap {
		if len(cvg.discoveredTrackIndexes) < h.anchorMinCount {
			continue
		}
		baseTrackIndexMap := make(map[int][]int)
		//lakeGroupSeq := newCharSeq()

		for _, trackIndex := range cvg.discoveredTrackIndexes {
			nodeIndex := baseTableTracks[trackIndex].nodeIndex
			if indexes, found := baseTrackIndexMap[nodeIndex]; !found {
				indexes = make([]int, 0, len(cvg.discoveredTrackIndexes))
				indexes = append(indexes, trackIndex)
				baseTrackIndexMap[nodeIndex] = indexes
			} else {
				indexes = append(indexes, trackIndex)
				baseTrackIndexMap[nodeIndex] = indexes
			}

		}

		jointMap := make(ColumnJointMap)
		hashValueMap := make(map[string]uint32)
		for nodeIndex, trackIndexes := range baseTrackIndexMap {
			jBaseSeq := newCharSeq()
			baseColumns := make(JointColumnPropMap)
			for _, trackIndex := range trackIndexes {
				seqId := baseTableTracks[trackIndex].seqId
				props := &JointColumnProps{
					seqId:    seqId,
					ColumnID: baseTableTracks[trackIndex].columnID,
				}
				baseColumns[seqId] = props
				jBaseSeq.Add(seqId)
			}

			jLakeSeq := newCharSeq()
			lakeColumns := make(JointColumnPropMap)
			for l := range nodes[nodeIndex].lakeColumns {
				columnId := nodes[nodeIndex].lakeColumns[l].ID.String
				seqId := h.seqMapper.mapLakeColumnID(columnId)
				jLakeSeq.Add(seqId)
				props := &JointColumnProps{
					seqId:    seqId,
					ColumnID: columnId,
				}
				lakeColumns[seqId] = props
			}

			baseKey := jBaseSeq.String(BASE_SEQ_FLAG)
			lakeKey := jLakeSeq.String(LAKE_SEQ_FLAG)

			{
				hsh.Reset()
				bs := []byte(nodes[nodeIndex].value)
				hsh.Write(bs)
				hashValueMap[baseKey] = hsh.Sum32()
			}

			jc := &ColumnJoint{
				baseKey:       baseKey,
				lakeKey:       lakeKey,
				uniqueValueBs: roaring.NewBitmap(),
				LakeColumns:   lakeColumns,
				BaseColumns:   baseColumns,
			}
			jc.uniqueValueBs.Add(hashValueMap[baseKey])
			jointMap[jc.Key()] = jc
		}
		if len(jointMap) < h.anchorMinCount {
			continue
		}

		groupKey := jointMap.GroupKey()
		//fmt.Println(gKey)
		if cg, found := h.cgsMap[groupKey]; !found {
			cg = &ColumnGroup{
				ID:           h.seqProvider.nextGroupSeqId(), // ++ len(r.cgsMap)+1),
				JointMap:     jointMap,
				lakeNumRowBs: roaring.NewBitmap(),
				baseNumRowBs: roaring.NewBitmap(),
			}
			cg.baseGroupKey, cg.lakeGroupKey = jointMap.CreateGroupKeys()

			if currentRowBlankLakeColumns != nil && len(currentRowBlankLakeColumns) > 0 {
				cg.BlankLakeColumns = make(JointColumnPropMap)
				for index := range currentRowBlankLakeColumns {
					columnId := currentRowBlankLakeColumns[index].ID.String
					seqId := h.seqMapper.mapLakeColumnID(columnId)
					cp := &JointColumnProps{
						ColumnID: columnId,
						seqId:    seqId,
					}
					cg.BlankLakeColumns[seqId] = cp
				}
			}

			cg.lakeNumRowBs.AddInt(rowNumber)
			cg.baseNumRowBs.Or(cvg.rowNumberBs)
			h.cgsMap[groupKey] = cg
		} else {
			for jKey, joint := range jointMap {
				if cj, found := cg.JointMap[jKey]; !found {
					panic("not found!")
				} else {
					cj.uniqueValueBs.Add(hashValueMap[joint.baseKey])
				}
			}
			/*
				currentBlankLakeMap := make(map[int]bool)
				for index := range currentRowBlankLakeColumns {
					seqId := h.seqMapper.mapLakeColumnID(currentRowBlankLakeColumns[index].ID.String)
					currentBlankLakeMap[seqId] = true
				}
				for seqId, _ := range cg.BlankLakeColumns {
					if _, found := currentBlankLakeMap[seqId]; !found {
						delete(cg.BlankLakeColumns, seqId)
					}
				}*/

			cg.lakeNumRowBs.AddInt(rowNumber)
			cg.baseNumRowBs.Or(cvg.rowNumberBs)
		}
	}

	//fmt.Printf("-----------------------------------------------\n")
	/*var blankLakeMap map[int]bool
	for _, cgs := range h.cgsMap {
		if blankLakeMap == nil {
			blankLakeMap = make(map[int]bool, len(cgs.BlankLakeColumns))
			for k := range cgs.BlankLakeColumns {
				blankLakeMap[k] = false
			}
		} else {
			for k := range blankLakeMap {
				if _, found := cgs.BlankLakeColumns[k]; !found {
					delete(blankLakeMap, k)
				}
			}
			if len(blankLakeMap) == 0 {
				blankLakeMap = nil
				break
			}
		}
	}
	if len(blankLakeMap) > 0 {
		for _, cg := range h.cgsMap {
			if len(cg.BlankLakeColumns) > 0 {
				for k := range blankLakeMap {
					delete(cg.BlankLakeColumns, k)
				}
			}
		}
	}*/
	return
}

func (h *DiscoveryingRowStrategy) collectIntersections(ctx context.Context, baseTableTracks rowTracks) (
	cvgMap map[string]*iCoverage, err error) {
	//	var BACKGROUND int = -2
	var KILLED int = -1
	cvgMap = make(map[string]*iCoverage)

	minRn := math.MaxInt64
	nextMinRn := math.MaxInt64

	activeTrackIndexes := make([]int, 0, len(baseTableTracks))

	columnsInRow := make(map[int]int)                  // rowNumber:coumnCount
	leadingIterators := make(map[int]iixs.IRnIterator) //trackIndex:Iterator
	followingStorages := make(map[int]iixs.IRnStorage)

	killIterator := func(index int) {
		//log.Infof("%v iterator has been exhausted in row #%v", cis[index].column, minRn)
		delete(leadingIterators, index)
		baseTableTracks[index].curRow = KILLED
	}
	for i, _ := range baseTableTracks {
		if baseTableTracks[i].mode == 'F' {
			followingStorages[i] = baseTableTracks[i].rnStorage
		} else if baseTableTracks[i].mode == 'I' {
			leadingIterators[i] = baseTableTracks[i].rnStorage.Iterator()
			activeTrackIndexes = append(activeTrackIndexes, i)
		}
	}
	if len(activeTrackIndexes) == 0 {
		return
	}
	if len(leadingIterators) == 0 || len(leadingIterators) < h.anchorMinCount { // TODO:: configuration?
		return
	}

	for {
		//Advance Row Iterators if needed
		//var blankCoumns map[string]bool

		for _, index := range activeTrackIndexes {
			if !leadingIterators[index].HasNext() {
				killIterator(index)
				continue
			}
			rowNumber := leadingIterators[index].Next()
			baseTableTracks[index].curRow = rowNumber
			columnsInRow[rowNumber]++ // increase # columns in the retrieved row number
		}

		if len(leadingIterators) == 0 || len(leadingIterators) < h.anchorMinCount { // TODO:: configuration?
			break
		}
		// Looking for nearest and next nearest row numbers ahead
		minRn, nextMinRn = baseTableTracks.mins()

		activeTrackIndexes = activeTrackIndexes[:0]

		if columnCount := columnsInRow[minRn]; columnCount > 1 {
			cseq := newCharSeq()
			//columnSeqIds := make([]int, 0, len(baseTableTracks))
			discoveredTrackIndexes := make([]int, 0, columnCount)
			//			blankTrackIndexes := make([]*roaring.Bitmap, len(baseTableTracks))

			registerTrackingColumn := func(trackIndex int) {
				if baseTableTracks[trackIndex].seqId == 0 {
					baseTableTracks[trackIndex].seqId = h.seqMapper.mapBaseColumnID(baseTableTracks[trackIndex].columnID)
				}
				cseq.Add(baseTableTracks[trackIndex].seqId)
			}
			for trackIndex, _ := range leadingIterators {
				if baseTableTracks[trackIndex].curRow == minRn {
					registerTrackingColumn(trackIndex)
					discoveredTrackIndexes = append(discoveredTrackIndexes, trackIndex)
				}
			}

			for trackIndex, storage := range followingStorages {
				if storage.Contains(minRn) {
					registerTrackingColumn(trackIndex)
					discoveredTrackIndexes = append(discoveredTrackIndexes, trackIndex)
				}
			}

			groupKey := cseq.String(BASE_SEQ_FLAG)
			cvg, found := cvgMap[groupKey]
			if !found {
				cvg = &iCoverage{
					discoveredTrackIndexes: discoveredTrackIndexes,
					rowNumberBs:            roaring.NewBitmap(),
				}
				cvgMap[groupKey] = cvg
			}
			cvg.rowNumberBs.AddInt(minRn)
		} else {
			for index, iterator := range leadingIterators {
				if baseTableTracks[index].curRow == minRn {
					activeTrackIndexes = append(activeTrackIndexes, index)
					iterator.AdvanceIfNeeded(nextMinRn)
				}
			}
		}

		//fmt.Printf("%v,",minRn)
		delete(columnsInRow, minRn)
	}
	/*coverages := make([]coverage, 0, len(cvgMap))
	for _, cvg := range cvgMap {
		coverages = append(coverages, cvg)
	}*/
	return
}

/*
func (h DiscoveryingRowStrategy) keySlice(csLen int, getFunc func(i int) string) (key string, n int) {
	maxColumnIndex := 0
	columnIndexes := make(map[int]struct{})
	for i := 0; i < csLen; i++ {
		ci := h.mapColumnName(getFunc(i))
		if ci > maxColumnIndex {
			maxColumnIndex = ci
		}
		columnIndexes[ci] = struct{}{}
	}
	keyChars := make([]byte, maxColumnIndex)
	for i := range keyChars {
		if _, f := columnIndexes[i+1]; f {
			keyChars[i] = 'T'
			n++
		} else {
			keyChars[i] = 'F'
		}
	}
	return string(keyChars), n
}
*/

func (r DiscoveryingRowStrategy) WriteTo(w io.Writer) (total int, err error) {
	var n = 0
	_, err = serde.IntWriteTo(w, int64(r.baseRowCount))
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	_, err = serde.IntWriteTo(w, int64(len(r.cgsMap)))
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	for _, gs := range r.cgsMap {
		n, err = gs.WriteTo(w)
		if err != nil {
			return -1, err
		}
		total += n
	}
	return
}
