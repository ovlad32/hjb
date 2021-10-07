package main

import (
	"database/sql"
	"strconv"

	"github.com/RoaringBitmap/roaring"
	sqlite "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
)

func sqliteToChar36(n int64) string {
	return strconv.FormatInt(n, 36)
}

type bitSetOrCard struct {
	bins [][]byte
}

func sqliteRoaringBitsetOrCard() *bitSetOrCard { return &bitSetOrCard{} }

func (b *bitSetOrCard) Step(bin1 []byte, bin2 []byte) {
	if b.bins == nil {
		b.bins = make([][]byte, 0, 2)
		b.bins = append(b.bins, bin1)
	}
	b.bins = append(b.bins, bin2)
}

func (b *bitSetOrCard) Done() (card uint64, err error) {
	var res *roaring.Bitmap
	for _, bin := range b.bins {
		b := roaring.NewBitmap()
		err = b.UnmarshalBinary(bin)
		if err != nil {
			return
		}
		if res == nil {
			res = b
		} else {
			res.Or(b)
		}
	}
	if res != nil {
		card = res.GetCardinality()

	}
	return
}

func sqliteRegister(driverName string) {
	sql.Register(driverName, &sqlite.SQLiteDriver{
		ConnectHook: func(conn *sqlite.SQLiteConn) error {
			if err := conn.RegisterFunc("to_char36", sqliteToChar36, true); err != nil {
				return err
			}
			if err := conn.RegisterAggregator("rbs_or_card", sqliteRoaringBitsetOrCard, true); err != nil {
				return err
			}
			return nil
		},
	})
}
func sqliteInit(wc WorkConfType) {
	db, err := sql.Open("sqlite3", wc.SqliteConnString)
	if err != nil {
		logger.Fatal(err)
	}
	defer db.Close()

	var ddls = []string{
		`create table if not exists mtd_dbs(
					id text, 
					name text,
					driver text,
					conn_string text,
					constraint mtd_dbs__pk primary key(id)
				) WITHOUT ROWID`,
		`create table if not exists mtd_tabs(
				  id text,
				  type text,
				  mtd_db_id text,
				  table_name text,
				  schema_name text,
			      row_count bigint,
				  size_bytes bigint,
				  dumping_query text,
				  constraint mtd_tabs__pk primary key(id)
			  ) WITHOUT ROWID`,
		`create table if not exists mtd_cols(
					id text,
					mtd_tab_id text,
					column_name text,
					position int,
					data_type text,
					runtime_data_type text,
					empty_count bigint,
					unique_count bigint,
					extent_mtd_col_id text,
					extent_position int,
					extent_count int,
					constraint cols__pk primary key(id)
				  ) WITHOUT ROWID`,
		`create table if not exists grps(
			        id INTEGER PRIMARY KEY AUTOINCREMENT,
					stage text,
					seq_key text,
					name text,
					lake_tag text,
					base_tag text,
					lake_agg_row_count int,
					base_agg_row_count int,
					jnt_cardinality int,
					agg_jnt_cardinality int,
					jnt_count int,
					lake_row_count int,
					base_row_count int,
					jc_1 int,
					jc_2 int,
					jc_3 int,
					jc_4 int,
					jc_5 int,
					jc_6 int,
					jc_7 int,
					jc_8 int,
					jc_9 int,
					jc_10 int,
					jc_11 int,
					jc_12 int,
					jc_13 int,
					jc_14 int,
					jc_15 int,
					jc_16 int,
					jc_17 int,
					jc_18 int,
					jc_19 int,
					jc_20 int
					/*,constraint grps__pk primary key(id) */
				)`,
		`create index if not exists grps__jc_1 on  grps(jc_1) where jc_1 is not null`,
		`create index if not exists grps__jc_2 on  grps(jc_2, jc_1) where jc_2 is not null`,
		`create index if not exists grps__jc_3 on  grps(jc_3, jc_2) where jc_3 is not null`,
		`create index if not exists grps__jc_4 on  grps(jc_4, jc_3) where jc_4 is not null`,
		`create index if not exists grps__jc_5 on  grps(jc_5, jc_4) where jc_5 is not null`,
		`create table if not exists grp_blank_cols(
				grp_id INTEGER,
				side text,
				csq int,
				mtd_col_id text,
				constraint grp_blank_cols__pk primary key(grp_id, mtd_col_id),
				constraint grp_blank_cols__uq0 unique(grp_id, side, csq)
			) WITHOUT ROWID`,
		`create index if not exists grp_blank_cols_col_grp_side on grp_blank_cols(mtd_col_id, grp_id, side)`,
		`create table if not exists grp_jnts(
			        id INTEGER PRIMARY KEY AUTOINCREMENT,
					grp_id INTEGER,
					seq_key text,
					lake_csq_key text,
					base_csq_key text,
					cardinality int,
					agg_cardinality int
					/*,constraint grp_jnts__pk primary key(seq_key, grp_id)*/
			  	)`,
		`create index if not exists grp_jnts__grp on grp_jnts(grp_id, id)`,
		`create table if not exists grp_jnt_cols(
					grp_jnt_id INTEGER,
					grp_id int,
					side text,
					csq int,
					mtd_col_id text
					,constraint grp_jnt_cols__pk primary key(grp_jnt_id, mtd_col_id)
					,constraint grp_jnt_cols__uq0 unique(grp_jnt_id, side, csq)
				)`,
		`create index if not exists grp_jnt_cols_col_jnt_side on grp_jnt_cols(mtd_col_id, grp_jnt_id, side)`,
		`create index if not exists grp_jnt_cols_col_grp on grp_jnt_cols(grp_id)`,
		`create table if not exists bin_data(
				entity_id int,
				entity_type text,
				bin blob,
				constraint col_grp_jnts__pk primary key(entity_id, entity_type)
				)  WITHOUT ROWID`,
		`create table if not exists grp_agg_chains(
			sps_grp_id int not null,
			grp_id int int not null,
			constraint grp_agg_chains__pk primary key(sps_grp_id, grp_id)
		) WITHOUT ROWID`,
		`create index if not exists grp_agg_chains__grp on grp_agg_chains(grp_id,sps_grp_id)`,
		`create table if not exists grp_jnt_agg_chains(
			sps_grp_jnt_id int not null,
			grp_jnt_id int int not null,
			grp_id int,
			sps_grp_id int,
			constraint grp_jnt_agg_chains__pk primary key(sps_grp_jnt_id, grp_jnt_id)
		) WITHOUT ROWID`,
		` create table if not exists grp_jnt_dvrgs(
			grp_id int, 
			sps_grp_id int, 
			grp_jnt_id int, 
			sps_grp_jnt_id int, 
			sps_grp_jnt_id_expected int,
			side text,
			csq int, 
			mtd_col_id text
		)`,
	}
	for _, ddlText := range ddls {
		_, err = db.Exec(ddlText)
		if err != nil {
			err = errors.Wrapf(err, "Executing DDL: %v"+ddlText)
			logger.Warn(err)
		}
	}
}
