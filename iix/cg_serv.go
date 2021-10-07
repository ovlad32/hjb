package iix

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/RoaringBitmap/roaring"
)

func CgSave(ctx context.Context, db *sql.DB, lakeTag, baseTag string,
	minLakeRowCount, minBaseRowCount int,
	cgsMap ColumnGroupMap) (err error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		logger.Fatal(err)
	}
	defer tx.Commit()
	grpInsert, err := tx.Prepare(`
		insert into grps(stage, name, seq_key, lake_tag, base_tag, 
			lake_row_count, lake_agg_row_count, 
			base_row_count, base_agg_row_count, 
			jnt_count, 
			jnt_cardinality, agg_jnt_cardinality) 
		values('I', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		logger.Fatalf("%+v", err)
	}

	grpBlankColInsert, err := tx.PrepareContext(ctx, `
		insert into grp_blank_cols (grp_id, side, csq, mtd_col_id)
		values(?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		logger.Fatalf("%+v", err)
	}
	grpJntInsert, err := tx.Prepare(`
		insert into grp_jnts(grp_id, seq_key, lake_csq_key, base_csq_key, cardinality, agg_cardinality)
		values(?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		logger.Fatalf("%+v", err)
	}
	grpJntColInsert, err := tx.Prepare(`
		insert into grp_jnt_cols(grp_jnt_id, grp_id, side, csq, mtd_col_id)
		values(?, ?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		logger.Fatalf("%+v", err)
	}

	bsInsert, err := tx.Prepare(`
	insert into bin_data(entity_id, entity_type, bin)
	values(?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		logger.Fatalf("%+v", err)
	}
	emptyString := sql.NullString{}
	for gKey, cg := range cgsMap {
		_ = gKey
		lakeRowCount := cg.lakeNumRowBs.GetCardinality()
		baseRowCount := cg.baseNumRowBs.GetCardinality()
		if (minLakeRowCount > 0 && lakeRowCount < uint64(minLakeRowCount)) ||
			(minBaseRowCount > 0 && baseRowCount < uint64(minBaseRowCount)) {
			continue
		}
		bs := roaring.NewBitmap()
		for _, jnt := range cg.JointMap {
			bs.Or(jnt.uniqueValueBs)
		}
		jCard := bs.GetCardinality()
		bs = nil

		rs, err := grpInsert.ExecContext(ctx, cg.ID /*gKey*/, emptyString, lakeTag, baseTag,
			lakeRowCount, lakeRowCount,
			baseRowCount, baseRowCount,
			len(cg.JointMap),
			jCard, jCard)
		if err != nil {
			tx.Rollback()
			logger.Fatalf("%+v", err)
		}
		grpId, err := rs.LastInsertId()
		if err != nil {
			tx.Rollback()
			logger.Fatalf("%+v", err)
		}
		bsBytes, err := cg.baseNumRowBs.MarshalBinary()
		if err != nil {
			tx.Rollback()
			logger.Fatalf("%+v", err)
		}
		bsInsert.ExecContext(ctx, grpId, "GBRN", bsBytes)

		bsBytes, err = cg.lakeNumRowBs.MarshalBinary()
		if err != nil {
			tx.Rollback()
			logger.Fatalf("%+v", err)
		}
		bsInsert.ExecContext(ctx, grpId, "GLRN", bsBytes)

		for _, c := range cg.BlankLakeColumns {
			_, err = grpBlankColInsert.ExecContext(ctx, grpId, "L", c.seqId, c.ColumnID)
			if err != nil {
				tx.Rollback()
				logger.Fatalf("%+v", err)
			}
		}
		{
			cards := make([]int, 0, len(cg.JointMap))
			for _, jnt := range cg.JointMap {
				cards = append(cards, int(jnt.uniqueValueBs.GetCardinality()))
			}
			updateText, updateParam := jointMapToDistributionUpdateText(cards)
			updateParam = append(updateParam, grpId)
			_, err = tx.ExecContext(ctx, updateText, updateParam...)
			if err != nil {
				tx.Rollback()
				logger.Fatalf("%+v", err)
			}
		}

		for jKey, jnt := range cg.JointMap {
			_ = jKey
			//jointId = fmt.Sprintf("%s:%s", cg.ID, jointId)
			card := jnt.uniqueValueBs.GetCardinality()
			//rs, err := grpJntInsert.ExecContext(ctx, grpId, jKey, jnt.lakeKey, jnt.baseKey, card)
			rs, err := grpJntInsert.ExecContext(ctx, grpId, emptyString, emptyString, emptyString, card, card)
			if err != nil {
				tx.Rollback()
				logger.Fatalf("%+v", err)
			}
			jntId, err := rs.LastInsertId()
			if err != nil {
				tx.Rollback()
				logger.Fatalf("%+v", err)
			}

			bsBytes, err = jnt.uniqueValueBs.MarshalBinary()
			if err != nil {
				tx.Rollback()
				logger.Fatalf("%+v", err)
			}
			bsInsert.ExecContext(ctx, jntId, "JHV", bsBytes)

			for _, c := range jnt.LakeColumns {
				_, err = grpJntColInsert.ExecContext(ctx, jntId, grpId, "L", c.seqId, c.ColumnID)
				if err != nil {
					logger.Fatalf("%+v for side L, cgId:%v, seqID:%v, colID:%v ", err, cg.ID, c.seqId, c.ColumnID)
				}
			}
			for _, c := range jnt.BaseColumns {
				_, err = grpJntColInsert.ExecContext(ctx, jntId, grpId, "B", c.seqId, c.ColumnID)
				if err != nil {
					logger.Fatalf("%+v for side B, cgId:%v, seqID:%v, colID:%v ", err, cg.ID, c.seqId, c.ColumnID)
				}
			}

		}

	}
	return
}

func jointMapToDistributionUpdateText(cardinalities []int) (updateText string, updateParams []interface{}) {
	var values = make([]int, 0, 20) //20 corresponds with jc_N in the grps table
	var columns = make([]string, 0, cap(values))
	var count = 0
	for _, v := range cardinalities {
		count++
		columns = append(columns, fmt.Sprintf("jc_%v=?", count))
		values = append(values, v)
		if count == cap(values) {
			break
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(values)))
	updateParams = make([]interface{}, 0, cap(values)+1)
	for _, jcard := range values {
		updateParams = append(updateParams, jcard)
	}
	updateText = "update grps set " + strings.Join(columns, ",") + " where id=?"
	return
}

func CgLoad(ctx context.Context, db *sql.DB, lakeTag, baseTag string, cgsMap ColumnGroupMap) (err error) {
	panic("not implemented")
}
