package iix

/*
type cgShoulderIndexes struct {
	jIndex int
	hIndex int
	iIndex int
}
type cgAggregationIndexes struct {
	sub   cgShoulderIndexes
	super cgShoulderIndexes
}


type jointRelationSlice []*jointRelation

func (js jointRelationSlice) withBlank() bool {
	for z := range js {
		if js[z].blankInclusion {
			return true
		}
	}
	return false
}*/

// looks like some refactoring
/*
func (jcs ColumnJoints) LakeColumnLen(next eqless.NextFunc) eqless.NextFunc {
	return eqless.Chain(
		func(n int) int { return len(jcs[n].LakeColumnIDs) },
		next,
	)

}
func (jcs ColumnJoints) BaseColumnLen(next eqless.NextFunc) eqless.NextFunc {
	return eqless.Chain(
		func(n int) int { return len(jcs[n].BaseColumnIDs) },
		next,
	)
}

func (jcs ColumnJoints) ColumnJointLen(next eqless.NextFunc) eqless.NextFunc {
	return eqless.Chain(
		func(n int) int { return len(jcs) },
		next,
	)
}
*/
/*
type lessFunc func(int, int) bool

func finish(i1, i2 int) bool { return false }
*/
/*
func (jcs ColumnJoints) byHColumnLen(nextFunc lessFunc) lessFunc {
	return lessFunc(func(i int, j int) bool {
		eq := len(jcs[i].LakeColumns) == len(jcs[j].LakeColumns)
		if eq {
			return nextFunc(i, j)
		}
		return len(jcs[i].LakeColumns) < len(jcs[j].LakeColumns)
	})
}

func (jcs ColumnJoints) byIColumnLen(nextFunc lessFunc) lessFunc {
	return lessFunc(func(i int, j int) bool {
		eq := len(jcs[i].BaseColumns) == len(jcs[j].BaseColumns)
		if eq {
			return nextFunc(i, j)
		}
		return len(jcs[i].BaseColumns) < len(jcs[j].BaseColumns)
	})
}
*/
/*
type SortOrder int

const ASC SortOrder = 1
const DESC SortOrder = 2

func (cgs ColumnGroupSlice) byTotalColumnCount(order SortOrder, nextFunc lessFunc) lessFunc {
	return lessFunc(func(i int, j int) bool {
		var ibc, ilc, jbc, jlc int
		for _, joint := range cgs[i].JointMap {
			ibc += len(joint.BaseColumns)
			ilc += len(joint.BaseColumns)
		}
		for _, joint := range cgs[j].JointMap {
			jbc += len(joint.BaseColumns)
			jlc += len(joint.BaseColumns)
		}
		if ibc == jbc && ilc == jlc {
			return nextFunc(i, j)
		} else if order == ASC {
			return ibc < jbc && ilc < jlc
		} else {
			return ibc > jbc && ilc > jlc
		}
	})
}
func (cgs ColumnGroupSlice) byRelationType(order SortOrder, nextFunc lessFunc) lessFunc {
	return lessFunc(func(i int, j int) bool {
		itype, _ := findJointRelations(cgs[i].JointMap, cgs[j].JointMap, false)
		if order == ASC && itype == superSet {
			return true
		} else if order == DESC && itype == subSet {
			return true
		}
		return nextFunc(i, j)
	})
}

func (cgs ColumnGroupSlice) byJoinCount(order SortOrder, nextFunc lessFunc) lessFunc {
	return lessFunc(func(i int, j int) bool {
		eq := len(cgs[i].JointMap) == len(cgs[j].JointMap)
		if eq {
			return nextFunc(i, j)
		}
		if order == ASC {
			return len(cgs[i].JointMap) < len(cgs[j].JointMap)
		} else {
			return len(cgs[i].JointMap) > len(cgs[j].JointMap)
		}
	})
}

func (cgs ColumnGroupSlice) byTotalColumns(order SortOrder, nextFunc lessFunc) lessFunc {
	return lessFunc(func(i int, j int) bool {
		iCount := 0
		jCount := 0
		for _, z := range cgs[i].JointMap {
			iCount += len(z.LakeColumns) + len(z.BaseColumns)
		}
		for _, z := range cgs[j].JointMap {
			jCount += len(z.LakeColumns) + len(z.BaseColumns)
		}
		eq := iCount == jCount
		if eq {
			return nextFunc(i, j)
		}
		if order == ASC {
			return iCount < jCount
		} else {
			return iCount > jCount
		}
	})
}
*/
