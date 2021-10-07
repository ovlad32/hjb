package iix

import (
	"context"
	"database/sql"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	pb "github.com/cheggaaa/pb"
	"github.com/pkg/errors"

	"fmt"

	"github.com/ovlad32/hjb/iixs"
)

type ColumnGroupAggregator struct {
	Rnsf                  iixs.IRnStorageFactory
	Bck                   IBlankChecker
	cgs                   ColumnGroupSlice
	baseColumnMap         map[string]int
	lakeColumnMap         map[string]int
	gen                   iSeqProvider
	BaseRowMinCount       int
	MergingThreadCount    int
	PersistingThreadCount int
	PersistingBatchSize   int
	BsCacheSize           int
	lrnCache              map[int]*roaring.Bitmap
	brnCache              map[int]*roaring.Bitmap
	jhvCache              map[int]*roaring.Bitmap
	//MinJointCount       int
	MergeBlanks bool
}

type aggregatedGroupStats struct {
	groupId             int
	spsGroupIds         []int
	lakeRowCount        uint64
	baseRowCount        uint64
	groupCardinality    uint64
	firstMaxCardinality uint64
	jointHvCard         map[int]uint64
	spsJointIds         map[int][]int
}

type aggPreparedStatements struct {
	conn           *sql.Conn
	tx             *sql.Tx
	grpCntUpdate   *sql.Stmt
	jntCntUpdate   *sql.Stmt
	grpChainInsert *sql.Stmt
	jntChainInsert *sql.Stmt
	jntDvrgInsert  *sql.Stmt
}

func openAggTransaction(ctx context.Context, db *sql.DB) (result aggPreparedStatements, err error) {
	result.conn, err = db.Conn(ctx)
	if err != nil {
		logger.Fatalf(err.Error())
	}

	result.tx, err = result.conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadUncommitted,
	})
	if err != nil {
		logger.Fatalf(err.Error())
	}

	result.grpCntUpdate, err = result.tx.PrepareContext(ctx, `
			update grps set stage = 'A', lake_agg_row_count = ?, base_agg_row_count = ?, agg_jnt_cardinality = ? where id = ?
		`)

	if err != nil {
		logger.Fatalf("%+v", err)
	}
	result.jntCntUpdate, err = result.tx.PrepareContext(ctx, `
			update grp_jnts set agg_cardinality = ? where id = ?
		`)
	if err != nil {
		logger.Fatalf("%+v", err)
	}
	result.grpChainInsert, err = result.tx.PrepareContext(ctx, `
		insert into grp_agg_chains(sps_grp_id, grp_id) values (?,?) on conflict do nothing
	`)
	if err != nil {
		logger.Fatalf("%+v", err)
	}
	result.jntChainInsert, err = result.tx.PrepareContext(ctx, `
		insert into grp_jnt_agg_chains(sps_grp_jnt_id, grp_jnt_id) values (?,?) on conflict do nothing
	`)

	result.jntDvrgInsert, err = result.tx.PrepareContext(ctx, `
		insert into grp_jnt_dvrgs(
			grp_id, sps_grp_id, 
			grp_jnt_id, sps_grp_jnt_id, sps_grp_jnt_id_expected, 
			side, csq, 
			mtd_col_id
		) values (?1,?2, ?3,?4,?5, ?6,?7, 
			(select mtd_col_id from grp_jnt_cols where grp_jnt_id = ?3 and side = ?6 and csq = ?7 limit 1))
	`)
	if err != nil {
		logger.Fatalf("%+v", err)
	}

	return
}

func (ps aggPreparedStatements) saveGroupStats(ctx context.Context, ags aggregatedGroupStats) (err error) {
	start := time.Now()
	_, err = ps.grpCntUpdate.ExecContext(ctx, ags.lakeRowCount, ags.baseRowCount, ags.groupCardinality, ags.groupId)
	if err != nil {
		logger.Fatalf("Update row counts for %v : %+v", ags.groupId, err)
	}
	for _, spsGrpId := range ags.spsGroupIds {
		_, err = ps.grpChainInsert.ExecContext(ctx, spsGrpId, ags.groupId)
		if err != nil {
			logger.Fatalf("Insert superset group references: %+v", err)
		}
	}
	var jCards = make([]int, 0, len(ags.jointHvCard))
	for jntId, card := range ags.jointHvCard {
		jCards = append(jCards, int(card))
		_, err = ps.jntCntUpdate.ExecContext(ctx, card, jntId)
		if err != nil {
			logger.Fatalf("Update unique counts: %+v", err)
		}
	}
	for jntId, spsJntIds := range ags.spsJointIds {
		for _, spsJntId := range spsJntIds {
			_, err = ps.jntChainInsert.ExecContext(ctx, spsJntId, jntId)
			if err != nil {
				logger.Fatalf("Insert superset group references: %+v", err)
			}
		}
	}
	cdUpdateText, cdParams := jointMapToDistributionUpdateText(jCards)
	cdParams = append(cdParams, ags.groupId)
	_, err = ps.tx.ExecContext(ctx, cdUpdateText, cdParams...)
	if err != nil {
		logger.Fatalf("Update group cardinality distribution: %+v", err)
	}

	{
		ms := int(time.Since(start).Minutes())
		if ms > 0 {
			logger.Printf("Group persisting time: %v ms", ms)
		}
	}
	return
}

func (ps aggPreparedStatements) saveJointDivergences(ctx context.Context, jds []jointDivergenceRow) (err error) {
	for i := range jds {
		_, err = ps.jntDvrgInsert.Exec(
			jds[i].sbsGrpId, jds[i].spsGrpId,
			jds[i].sbsJntId, jds[i].spsJntId, jds[i].spsJntIdExpected,
			jds[i].side, jds[i].csq,
		)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
	}
	return
}

func (ps aggPreparedStatements) commit() (err error) {
	ps.tx.Commit()
	ps.conn.Close()
	return
}
func (ps aggPreparedStatements) rollback() (err error) {
	ps.tx.Rollback()
	ps.conn.Close()
	return
}

func (agg ColumnGroupAggregator) getBs(ctx context.Context, cache map[int]*roaring.Bitmap, binFetch func() *sql.Stmt, id int, idType string) (bs *roaring.Bitmap, err error) {
	//logger.Fatalf("bsFetch for %v and %v", entityId, entityType)
	if ibs, ok := cache[id]; ok {
		if ibs != nil {
			//bs = ibs.(*roaring.Bitmap)
			bs = ibs
			return
		}
	}
	bsRow := binFetch().QueryRowContext(ctx, id, idType)

	if err = bsRow.Err(); err == sql.ErrNoRows {
		//Do nothing
		logger.Printf("No BS for %v and %v", id, idType)
	} else if err != nil {
		logger.Fatalf("%+v", err)
	} else {
		var buff []byte
		err = bsRow.Scan(&buff)
		if err != nil {
			err = errors.Wrapf(err, " bsRow.Scan(&buff) for %v and %v", id, idType)
			logger.Fatalf("%+v", err)
		}
		bs = roaring.NewBitmap()
		err = bs.UnmarshalBinary(buff)
		if err != nil {
			logger.Fatalf("%+v", err)
		}
		cache[id] = bs
	}
	return
}

type aggMatrixSqlText struct {
	dropTableText        string
	dropViewText         string
	createTableText      string
	createCountIndexText string
	createGroupIndexText string
	createViewText       string
	populateText         string
	fetchText            string
	columnIndexes        []string
	lakeColumnCount      int
	baseColumnCount      int
}

func buildAggMatrixSQLTexts(ctx context.Context, db *sql.DB) (result aggMatrixSqlText, err error) {
	var bothSeqCount = 0
	var SideCount = make(map[string]int)
	{
		rows, err := db.QueryContext(ctx, "select max(csq), side from grp_jnt_cols group by side")
		if err != nil {
			logger.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
			var n int
			var side string
			err = rows.Scan(&n, &side)
			if err != nil {
				logger.Fatal(err)
			}
			SideCount[side] = n
			bothSeqCount = bothSeqCount + n
			if side == "L" {
				result.lakeColumnCount = n
			} else {
				result.baseColumnCount = n
			}
		}
	}

	var matrixColumns []string = make([]string, 0, bothSeqCount)
	var insertCases []string = make([]string, 0, bothSeqCount)
	var wherePairs []string = make([]string, 0, bothSeqCount)
	var selectPairs []string = make([]string, 0, bothSeqCount)
	result.columnIndexes = make([]string, 0, bothSeqCount)
	for _, side := range []string{"L", "B"} {
		for seq := 1; seq <= SideCount[side]; seq++ {
			var alias = fmt.Sprintf("%v%v", side, seq)
			matrixColumns = append(matrixColumns, fmt.Sprintf("%v int", alias))
			insertCases = append(insertCases, fmt.Sprintf("max(case when side='%v' and csq=%v then grp_jnt_id end)", side, seq))
			wherePairs = append(wherePairs, fmt.Sprintf("(sbs.%v is null or (sbs.%v is not null and sps.%v is not null))", alias, alias, alias))
			selectPairs = append(selectPairs, fmt.Sprintf("sbs.%v as sbs_%v, sps.%v as sps_%v ", alias, alias, alias, alias))
			result.columnIndexes = append(result.columnIndexes, fmt.Sprintf("create index grp_matrix__%v on grp_matrix(case when %v is not null then 'Y' end) where %v is not null", alias, alias, alias))
		}
	}
	result.dropTableText = "drop table if exists grp_matrix;"
	result.dropViewText = "drop view if exists grp_matrix_agg;"
	result.createTableText = "create table grp_matrix(grp_id int, lake_cnt int, base_cnt int," +
		strings.Join(matrixColumns, ",") + ");"
	result.populateText = "insert into grp_matrix \n" +
		"select grp_id, count(case when side = 'L' then 1 end) as lake_cnt, count(case when side = 'B' then 1 end) as base_cnt," +
		strings.Join(insertCases, "\n,") + " from grp_jnt_cols group by grp_id"
	result.createViewText = "create view grp_matrix_agg as select sbs.grp_id as sbs_grp_id, sps.grp_id as sps_grp_id,\n" +
		strings.Join(selectPairs, "\n,") + " from grp_matrix sbs, grp_matrix sps \n" +
		" where sbs.grp_id <> sps.grp_id \n" +
		" and sbs.lake_cnt <= sps.lake_cnt and sbs.base_cnt <= sps.base_cnt \n " +
		" and " + strings.Join(wherePairs, "\n and") + ";"
	result.createCountIndexText = "create index grp_matrix__side_cnt on grp_matrix(lake_cnt, base_cnt)"
	result.createGroupIndexText = "create index grp_matrix__grp on grp_matrix(grp_id)"
	result.fetchText = "select * from grp_matrix_agg where sbs_grp_id = ?"

	return
}

type aggMatrixRow struct {
	sbsGrpId int
	spsGrpId int
	jntIdMap map[int]int
}

type jointDivergenceRow struct {
	sbsGrpId         int
	spsGrpId         int
	sbsJntId         int
	spsJntId         int
	spsJntIdExpected int
	side             string
	csq              int
}

type fetchResult struct {
	mxRows []aggMatrixRow
	jdRows []jointDivergenceRow
	stats  aggregatedGroupStats
}

func (agg ColumnGroupAggregator) merge(ctx context.Context, db *sql.DB, rows []aggMatrixRow) (result aggregatedGroupStats, err error) {
	if len(rows) == 0 {
		return
	}
	var conn *sql.Conn
	var binFetch *sql.Stmt
	getFetch := func() *sql.Stmt {
		if binFetch != nil {
			return binFetch
		}
		conn, err = db.Conn(ctx)
		if err != nil {
			logger.Fatalf("%+v", err)
		}
		binFetch, err = conn.PrepareContext(ctx, `
			select bin from bin_data where entity_id = ? and entity_type = ? and chunk = 0
		`)
		return binFetch
	}

	//	start := time.Now()

	result.groupId = rows[0].sbsGrpId
	result.spsGroupIds = make([]int, 0, len(rows))
	result.spsJointIds = make(map[int][]int)
	sbJntBs := make(map[int]*roaring.Bitmap)
	//bs =  agg.lrnCache[ar.spsGrpId].Clone()
	lakeRnBs, err := agg.getBs(ctx, agg.lrnCache, getFetch, result.groupId, "GLRN")
	lakeRnBs = lakeRnBs.Clone()
	//lakeRnBs := agg.lrnCache[result.groupId].Clone()
	if err != nil {
		logger.Fatalf("%+v", err)
	}

	baseRnBs, err := agg.getBs(ctx, agg.brnCache, getFetch, result.groupId, "GBRN")
	baseRnBs = baseRnBs.Clone()
	//baseRnBs := agg.brnCache[result.groupId].Clone()
	if err != nil {
		logger.Fatalf("%+v", err)
	}
	//aggHvBs := roaring.NewBitmap()

	var prevSpGrpId int = math.MinInt64
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].spsGrpId < rows[j].spsGrpId
	})

	for _, ar := range rows {
		if prevSpGrpId != ar.spsGrpId {
			var bs *roaring.Bitmap
			bs, err = agg.getBs(ctx, agg.lrnCache, getFetch, ar.spsGrpId, "GLRN")
			//bs = agg.lrnCache[ar.spsGrpId]
			if err != nil {
				logger.Fatalf("%+v", err)
			}
			lakeRnBs.Or(bs)

			bs, err = agg.getBs(ctx, agg.brnCache, getFetch, ar.spsGrpId, "GBRN")
			//bs = agg.brnCache[ar.spsGrpId]
			if err != nil {
				logger.Fatalf("%+v", err)
			}
			baseRnBs.Or(bs)
			result.spsGroupIds = append(result.spsGroupIds, ar.spsGrpId)
			prevSpGrpId = ar.spsGrpId
		}

		for sbsJntId, spsJntId := range ar.jntIdMap {
			{
				sbHvBs, found := sbJntBs[sbsJntId]
				if !found {
					sbHvBs, err = agg.getBs(ctx, agg.jhvCache, getFetch, sbsJntId, "JHV")
					sbHvBs = sbHvBs.Clone()
					//sbHvBs = agg.brnCache[sbsJntId].Clone()
					if err != nil {
						logger.Fatalf("%+v", err)
					}
					sbJntBs[sbsJntId] = sbHvBs
					result.spsJointIds[sbsJntId] = make([]int, 0, len(rows))
				}
				var bs *roaring.Bitmap
				bs, err = agg.getBs(ctx, agg.jhvCache, getFetch, spsJntId, "JHV")
				//bs = agg.hjvCache[spsJntId]
				if err != nil {
					logger.Fatalf("%+v", err)
				}
				sbHvBs.Or(bs)
				result.spsJointIds[sbsJntId] = append(result.spsJointIds[sbsJntId], spsJntId)
			}
		}

		result.lakeRowCount = lakeRnBs.GetCardinality()
		result.baseRowCount = baseRnBs.GetCardinality()
		result.jointHvCard = make(map[int]uint64)
		result.groupCardinality = 0
		result.firstMaxCardinality = 0

		for jntId, bs := range sbJntBs {
			jCard := bs.GetCardinality()
			result.jointHvCard[jntId] = jCard
			result.groupCardinality += jCard
			if result.firstMaxCardinality < jCard {
				result.firstMaxCardinality = jCard
			}
		}
		// {
		// 	mn := int(time.Since(start).Minutes())
		// 	if mn > 0 {
		// 		logger.Printf("Group processing time: %v", time.Since(start))
		// 	}
		//}
		//start = time.Now()
	}
	/*if binFetch != nil {
		binFetch.Close()
	}
	if conn != nil {
		conn.Close()
	}*/
	return
}

func (agg ColumnGroupAggregator) fetchMatrix(ctx context.Context, db *sql.DB, mxSQL *aggMatrixSqlText, grpId int, groupCount int) (r fetchResult, err error) {
	var rowValues = make([]sql.NullInt64, 2+2*(mxSQL.lakeColumnCount+mxSQL.baseColumnCount))
	var valuePointers = make([]interface{}, 2+2*(mxSQL.lakeColumnCount+mxSQL.baseColumnCount))
	for i := range rowValues {
		valuePointers[i] = &rowValues[i]
	}
	rs, err := db.QueryContext(ctx, mxSQL.fetchText, grpId)
	if err != nil {
		logger.Fatal(err)
	}
	for rs.Next() {
		err = rs.Scan(valuePointers...)
		if err != nil {
			logger.Fatal(err)
		}
		mxRow := aggMatrixRow{
			sbsGrpId: int(rowValues[0].Int64),
			spsGrpId: int(rowValues[1].Int64),
			jntIdMap: make(map[int]int),
		}
		var jds []jointDivergenceRow
		for i := 2; i < len(rowValues); i += 2 {
			if rowValues[i].Valid && rowValues[i+1].Valid {
				sbsJntId := int(rowValues[i].Int64)
				spsJntId := int(rowValues[i+1].Int64)
				storedSpsJntId, ok := mxRow.jntIdMap[sbsJntId]
				if ok && storedSpsJntId != spsJntId {
					jd := jointDivergenceRow{
						sbsGrpId:         mxRow.sbsGrpId,
						spsGrpId:         mxRow.spsGrpId,
						sbsJntId:         sbsJntId,
						spsJntId:         spsJntId,
						spsJntIdExpected: storedSpsJntId,
						side:             "L",
						csq:              (i / 2),
					}
					if jd.csq > mxSQL.lakeColumnCount {
						jd.side = "B"
						jd.csq = jd.csq - mxSQL.lakeColumnCount
					}
					//fmt.Printf("Skip %v,%v", mxRow.sbsGrpId, mxRow.spsGrpId)
					if jds == nil {
						jds = make([]jointDivergenceRow, 0, mxSQL.lakeColumnCount+mxSQL.baseColumnCount)
					}
					jds = append(jds, jd)
				} else {
					mxRow.jntIdMap[sbsJntId] = spsJntId
				}
			}
		}
		if len(jds) > 0 {
			if r.jdRows == nil {
				r.jdRows = make([]jointDivergenceRow, 0, groupCount)
			}
			r.jdRows = append(r.jdRows, jds...)
		} else {
			if r.mxRows == nil {
				r.mxRows = make([]aggMatrixRow, 0, groupCount)
			}
			r.mxRows = append(r.mxRows, mxRow)
		}
	}
	rs.Close()
	return
}

func (agg ColumnGroupAggregator) Aggregate(db *sql.DB) (r ColumnGroupAggregator, err error) {
	ctx := context.TODO()

	if agg.BsCacheSize <= 0 {
		agg.BsCacheSize = 1000
	}
	logger.Printf("BsCacheSize = %v", agg.BsCacheSize)
	if agg.MergingThreadCount == 0 {
		agg.MergingThreadCount = 1
	}
	logger.Printf("MergingThreadCount = %v", agg.MergingThreadCount)
	if agg.PersistingThreadCount <= 0 {
		agg.PersistingThreadCount = 1
	}
	logger.Printf("PersistingThreadCount = %v", agg.PersistingThreadCount)
	if agg.PersistingBatchSize <= 0 {
		agg.PersistingBatchSize = 1000
	}
	logger.Printf("PersistingBatchSize = %v", agg.PersistingBatchSize)
	agg.lrnCache = make(map[int]*roaring.Bitmap)
	agg.brnCache = make(map[int]*roaring.Bitmap)
	agg.jhvCache = make(map[int]*roaring.Bitmap)
	/*agg.brnCache, err = lru.NewARC(agg.BsCacheSize)
	if err != nil {
		logger.Fatalf(err.Error())
	}
	agg.lrnCache, err = lru.NewARC(agg.BsCacheSize)
	if err != nil {
		logger.Fatalf(err.Error())
	}
	agg.hjvCache, err = lru.NewARC(agg.BsCacheSize)
	if err != nil {
		logger.Fatalf(err.Error())
	}*/

	aggStartTime := time.Now()
	//db.SetMaxOpenConns(agg.PersistingThreadCount + 1 + 1)

	if err != nil {
		logger.Fatalf(err.Error())
	}

	logger.Printf("Counting number of column groups")

	var groupIds []int64
	rsg, err := db.QueryContext(ctx, "select id,count(*) over () as total from grps")
	if err != nil {
		logger.Fatalf(err.Error())
	}
	for rsg.Next() {
		var grpId, total int64
		err = rsg.Scan(&grpId, &total)
		if groupIds == nil {
			groupIds = make([]int64, 0, total)
		}
		groupIds = append(groupIds, grpId)
	}
	rsg.Close()
	if len(groupIds) == 0 {
		logger.Printf("Column groups have not been found. Nothing to aggregate")
		return
	}
	logger.Printf("Start aggregating %v column groups ", len(groupIds))

	rsb, err := db.QueryContext(ctx, "select entity_id, entity_type, bin from bin_data")
	if err != nil {
		logger.Fatalf(err.Error())
	}
	logger.Printf("loading index data")
	for rsb.Next() {
		var eid int
		var etype string
		var eblob []byte
		err = rsb.Scan(&eid, &etype, &eblob)
		if err != nil {
			logger.Fatalf(err.Error())
		}
		bs := roaring.NewBitmap()
		err = bs.UnmarshalBinary(eblob)
		if err != nil {
			logger.Fatalf(err.Error())
		}
		switch etype {
		case "JHV":
			agg.jhvCache[eid] = bs
		case "GLRN":
			agg.lrnCache[eid] = bs
		case "GBRN":
			agg.brnCache[eid] = bs
		default:
			err = errors.Errorf("Unkown binary entity type %s", etype)
		}
		if err != nil {
			logger.Fatalf(err.Error())
		}
	}
	rsb.Close()

	logger.Printf("Building aggregation matrix")
	_, err = db.ExecContext(ctx, `update grps set stage = 'I'`)
	if err != nil {
		logger.Fatalf(err.Error())
	}

	_, err = db.ExecContext(ctx, "delete from grp_agg_chains")
	if err != nil {
		logger.Fatalf(err.Error())
	}
	_, err = db.ExecContext(ctx, "delete from grp_jnt_agg_chains")
	if err != nil {
		logger.Fatalf(err.Error())
	}
	_, err = db.ExecContext(ctx, "delete from grp_jnt_dvrgs")
	if err != nil {
		logger.Fatalf(err.Error())
	}

	_, err = db.ExecContext(ctx, "update grps set lake_agg_row_count = lake_row_count, base_agg_row_count = base_row_count, agg_jnt_cardinality = jnt_cardinality")
	if err != nil {
		logger.Fatalf(err.Error())
	}

	_, err = db.ExecContext(ctx, "update grp_jnts set agg_cardinality = cardinality")
	if err != nil {
		logger.Fatalf(err.Error())
	}

	mxSQL, err := buildAggMatrixSQLTexts(ctx, db)
	if err != nil {
		logger.Fatalf(err.Error())
	}
	_, err = db.ExecContext(ctx, mxSQL.dropViewText)
	if err != nil {
		logger.Fatalf(err.Error())
	}

	_, err = db.ExecContext(ctx, mxSQL.dropTableText)
	if err != nil {
		logger.Fatalf(err.Error())
	}

	_, err = db.ExecContext(ctx, mxSQL.createTableText)
	if err != nil {
		logger.Fatalf(err.Error())
	}

	_, err = db.ExecContext(ctx, mxSQL.createViewText)
	if err != nil {
		logger.Fatalf(err.Error())
	}

	_, err = db.ExecContext(ctx, mxSQL.populateText)
	if err != nil {
		logger.Fatalf(err.Error())
	}
	_, err = db.ExecContext(ctx, mxSQL.createCountIndexText)
	if err != nil {
		logger.Fatalf(err.Error())
	}

	_, err = db.ExecContext(ctx, mxSQL.createGroupIndexText)
	if err != nil {
		logger.Fatalf(err.Error())
	}

	// for _, columnIndexText := range mxSQL.columnIndexes {
	// 	_, err = db.ExecContext(ctx, columnIndexText)
	// 	if err != nil {
	// 		logger.Fatalf(errors.Wrapf(err, "indexing agg matrix ", columnIndexText).Error())
	// 	}
	// }

	//fmt.Printf("%v,%v", mxSQL.lakeColumnCount, mxSQL.baseColumnCount)

	var grpIdChan = make(chan int)
	var grpIdWg sync.WaitGroup

	var fetchResultChan = make(chan fetchResult, 1)
	var frWg sync.WaitGroup
	//var jdsChan = make(chan []jointDivergenceRow, 1)
	//	var jdsWg sync.WaitGroup

	var groupStatsChan = make(chan fetchResult, agg.PersistingBatchSize)
	var gsWg sync.WaitGroup
	var gssWg sync.WaitGroup

	var uiMxr = pb.New(len(groupIds))
	var uiBarStart sync.Once
	uiMxr.ShowPercent = true
	uiMxr.ShowBar = true
	uiMxr.ShowCounters = true
	uiMxr.ShowSpeed = true
	uiMxr.ShowElapsedTime = true
	uiMxr.ShowTimeLeft = true
	uiMxr.SetRefreshRate(time.Second)
	var uiSave = pb.New(agg.PersistingBatchSize)
	uiSave.ShowPercent = true
	uiSave.ShowBar = true
	uiSave.ShowCounters = true
	uiSave.ShowSpeed = true
	uiSave.ShowElapsedTime = false
	uiSave.ShowTimeLeft = false
	uiSave.SetRefreshRate(time.Second)
	var uiPool = pb.NewPool(uiMxr, uiSave)

	for t := 0; t < agg.MergingThreadCount; t++ {
		grpIdWg.Add(1)
		go func() {
			for grpId := range grpIdChan {
				func(grpId int) {
					fr, err := agg.fetchMatrix(ctx, db, &mxSQL, grpId, len(groupIds))
					if err != nil {
						logger.Fatal(err)
					}
					uiBarStart.Do(func() {
						uiPool.Start()
					})
					fetchResultChan <- fr
				}(grpId)
			}
			grpIdWg.Done()
		}()

		frWg.Add(1)
		go func(rowCountMin uint64) {
			for fr := range fetchResultChan {
				func(fr fetchResult) {
					var err error
					fr.stats, err = agg.merge(ctx, db, fr.mxRows)
					if err != nil {
						logger.Fatal(err)
					}
					groupStatsChan <- fr
					uiMxr.Increment()

				}(fr)
			}
			frWg.Done()
		}(uint64(agg.BaseRowMinCount))
	}
	flushBatch := func(b []fetchResult) {

		uiSave.SetTotal(len(b))
		uiSave.Set(0)
		aggtx, err := openAggTransaction(ctx, db)
		if err != nil {
			logger.Fatalf(err.Error())
		}
		for _, fr := range b {
			if len(fr.stats.jointHvCard) > 0 {
				err = aggtx.saveGroupStats(ctx, fr.stats)
				if err != nil {
					aggtx.rollback()
					logger.Fatal(err)
				}
			}
			if fr.jdRows != nil {
				err := aggtx.saveJointDivergences(ctx, fr.jdRows)
				if err != nil {
					aggtx.rollback()
					logger.Fatal(err)
				}
			}
			uiSave.Increment()
		}

		aggtx.commit()
		uiSave.Update()
		gssWg.Done()
		//logger.Printf("Group batch persisting time: %v", time.Since(start))
	}
	newBatch := func() []fetchResult {
		return make([]fetchResult, 0, agg.PersistingBatchSize)
	}
	for t := 0; t < agg.PersistingThreadCount; t++ {
		gsWg.Add(1)
		go func() {
			var batch = newBatch()
			for fr := range groupStatsChan {
				if len(fr.stats.jointHvCard) > 0 && fr.stats.firstMaxCardinality > 1 {
					batch = append(batch, fr)
					if len(batch) == cap(batch) {
						gssWg.Wait()
						gssWg.Add(1)
						go flushBatch(batch)
						batch = newBatch()
					}
				}
			}
			if len(batch) > 0 {
				gssWg.Wait()
				gssWg.Add(1)
				go flushBatch(batch)
				batch = nil
			}
			gssWg.Wait()
			gsWg.Done()
		}()
	}

	logger.Printf("Processing aggregation matrix")
	//	uiBar.SetWriter(os.Stdout)

	for _, grpId := range groupIds {
		//gssWg.Wait() // long running selects block inserts
		grpIdChan <- int(grpId)
	}
	close(grpIdChan)
	grpIdWg.Wait()

	close(fetchResultChan)
	frWg.Wait()

	close(groupStatsChan)
	gsWg.Wait()

	uiMxr.Update()

	_, err = db.ExecContext(ctx, `
		update grp_jnt_agg_chains set 
			grp_id = (select grp_id from grp_jnts j where j.id = grp_jnt_id),
			sps_grp_id = (select grp_id from grp_jnts j where j.id = sps_grp_jnt_id)
		`)
	if err != nil {
		logger.Fatalf(err.Error())
	}
	_, err = db.ExecContext(ctx, `
		update grps set stage = 'D' where base_agg_row_count < ?1
	`, agg.BaseRowMinCount)
	if err != nil {
		logger.Fatalf(err.Error())
	}

	logger.Infof("Finish column group aggregation. Total time: %v", time.Since(aggStartTime))
	return
}
