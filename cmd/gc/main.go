package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
)

var sqlDbFile string = "file:./hjb.db3?_sync=OFF&_journal_mode=MEMORY&_locking_mode=NORMAL&cache=private&_vacuum=none"
var logger = log.New()

func main() {

	db, err := sql.Open("sqlite3", sqlDbFile)

	if err != nil {
		logger.Fatal(err)
	}
	defer db.Close()
	var bothCsqCount = 0
	var SideCount = make(map[string]int)
	{
		rows, err := db.Query(`select max(csq), side from grp_jnt_cols group by side`)
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
			bothCsqCount = bothCsqCount + n
		}
	}

	var sideCol []string = make([]string, 0, bothCsqCount)
	var caseCol []string = make([]string, 0, bothCsqCount)
	var aggCondCol []string = make([]string, 0, bothCsqCount)
	var aggSideCol []string = make([]string, 0, bothCsqCount)
	for side, count := range SideCount {
		for csq := 1; csq <= count; csq++ {
			var alias = fmt.Sprintf("%v%v", side, csq)
			sideCol = append(sideCol, alias)
			caseCol = append(caseCol, fmt.Sprintf("max(case when side='%v' and csq=%v then grp_jnt_id end)", side, csq))
			aggCondCol = append(aggCondCol, fmt.Sprintf("(sbs.%v is null or (sbs.%v is not null and sps.%v is not null))", alias, alias, alias))
			aggSideCol = append(aggSideCol, fmt.Sprintf("sbs.%v as sbs_%v, sps.%v as sps_%v ", alias, alias, alias, alias))

		}
	}
	/*
		matrixSqlText = matrixSqlText + "," + strings.Join(sideCol, ",")
		insertSqlText = insertSqlText + "\n," +
		aggSelectSqlText = aggSelectSqlText +"\n and " +


	*/
	var matrixSqlText = `drop table if exists grp_matrix; 
		create table grp_matrix(grp_id,` + strings.Join(sideCol, ",") + `);`
	var insertSqlText = `insert into grp_matrix
	    select grp_id
		,` + strings.Join(caseCol, "\n,") + `
		 from grp_jnt_cols group by grp_id`
	var aggSelectSqlText = `select 
	     sbs.grp_id as sbs_grp_id
		 ,sps.grp_id as sps_grp_id
	     ,` + strings.Join(aggSideCol, "\n,") + `
	     from grp_matrix sbs, grp_matrix sps
		 where sbs.grp_id > sps.grp_id 
		  and ` + strings.Join(aggCondCol, "\n and")
	//fmt.Println(insertSqlText)
	fmt.Println(aggSelectSqlText)

	_, err = db.Exec(matrixSqlText + insertSqlText)
	if err != nil {
		logger.Fatal(err)
	}

	rows, err := db.Query(aggSelectSqlText)
	if err != nil {
		logger.Fatal(err)
	}
	var rowValues = make([]sql.NullInt64, 2+2*bothCsqCount)
	var valuePointers = make([]interface{}, 2+2*bothCsqCount)
	for i := range rowValues {
		valuePointers[i] = &rowValues[i]
	}
	type aggRow struct {
		sbsGrpId int
		spsGrpId int
		jntIdMap map[int]int
	}
	//var aggRows = make([]aggRow, 0)
	for rows.Next() {
		err = rows.Scan(valuePointers...)
		if err != nil {
			logger.Fatal(err)
		}
		r := aggRow{
			sbsGrpId: int(rowValues[0].Int64),
			spsGrpId: int(rowValues[1].Int64),
			jntIdMap: make(map[int]int),
		}
		for i := 2; i < len(rowValues); i+=2 {
			if rowValues[i].Valid && rowValues[i+1].Valid {
				sbsJntId := int(rowValues[i].Int64)
				spsJntId := int(rowValues[i+1].Int64)
				
				keptJntId, ok := r.jntIdMap[sbsJntId]
				if ok && keptJntId != spsJntId {
					fmt.Printf("Skip %v,%v", r.sbsGrpId, r.spsGrpId)
					continue
				}
				r.jntIdMap[sbsJntId] = spsJntId
			}
		}
		fmt.Printf("%v\n", r)
	}
}
