package iix
/*
import "fmt"

type setType int

const (
	emptySet setType = iota
	subSet
	superSet
	fullSet
)

func (s *setType) Flip() {
	if *s == superSet {
		*s = subSet
	} else if *s == subSet {
		*s = superSet
	}
}

func (i setType) String() string {
	switch i {
	case emptySet:
		return "emptySet"
	case subSet:
		return "subSet"
	case superSet:
		return "superSet"
	case fullSet:
		return "fullSet"
	}
	panic(fmt.Sprintf("unknown name for value %d", i))
}

func charInclusion(s1, s2 string, onChar byte) (result setType) {
	result = emptySet
	l1, l2 := len(s1), len(s2)
	if l1 == l2 {
		if l1 == 0 {
			return
		}
		if l1 == 1 && s1[0] != s2[0] {
			return
		}
	}
	result = fullSet
	if l1 > l2 {
		result = superSet
	} else if l1 < l2 {
		result = subSet
	}

	for i := 0; i < l1 && i < l2; i++ {
		switch result {
		case superSet:
			if s1[i] != onChar && s2[i] == onChar {
				return emptySet
			}
		case subSet:
			if s1[i] == onChar && s2[i] != onChar {
				return emptySet
			}
		default:
			if s1[i] == onChar && s2[i] != onChar {
				result = superSet
			} else if s1[i] != onChar && s2[i] == onChar {
				result = subSet
			}
		}
	}
	return
}

func nodeInclusion(k1, k2 NodeKeyer) setType {
	base := charInclusion(k1.BaseKey(), k2.BaseKey(), BASE_SEQ_FLAG)
	if base == emptySet {
		return base
	}
	lake := charInclusion(k1.LakeKey(), k2.LakeKey(), LAKE_SEQ_FLAG)
	if lake == emptySet {
		return lake
	}
	if base == fullSet {
		return lake
	}
	if lake == fullSet {
		return base
	}
	if base != lake {
		return emptySet
	}
	// here base == lake
	return base
}

type jointRelation struct {
	super     NodeKeyer
	sub       NodeKeyer
	inclusion setType
}
type jointRelationSlice []*jointRelation

func findJointRelations(jm1, jm2 NodeKeyerMap, withRelations bool) (setType, jointRelationSlice) {
	findRelations := func(greater, sparser NodeKeyerMap) (ret setType, rels jointRelationSlice) {
		if withRelations {
			rels = make(jointRelationSlice, 0, greater.Len())
		}
		subsetCount := 0
		supersetCount := 0
		fullCount := 0

		quit := greater.ForEach(func(largeKey string, node1 NodeKeyer) bool {
			//fmt.Printf("%v\n", greaterKey)
			rel := &jointRelation{
				super:     node1,
				inclusion: emptySet,
			}
			quit := sparser.ForEach(func(sparserKey string, node2 NodeKeyer) bool {
				//fmt.Printf(" -- %v\n", sparserKey)
				// ret is what node1 towards node2 is
				// But we need reverse
				ret = nodeInclusion(node2, node1)
				if ret == emptySet {
					//fmt.Printf("%v - %v-%v\n", j2result, greaterKey, sparserKey)
				} else if rel.inclusion == emptySet {
					//fmt.Printf("%-20v%-20v\n%-20v%-20v\n%v---------------------\n\n", node1.BaseKey(), node1.LakeKey(), node2.BaseKey(), node2.LakeKey(), ret)
					rel.inclusion = ret
					rel.sub = node2
				} else if rel.inclusion != ret {
					rel.inclusion = emptySet
					return true
				}
				return false
			})
			//fmt.Printf("%v %v\n", quit, j2result)
			if quit {
				return true
			}
			switch rel.inclusion {
			case fullSet:
				fullCount++
				//fmt.Printf("fullCount:%v\n", fullCount)
			case subSet:
				subsetCount++
				//fmt.Printf("subsetCount:%v\n", subsetCount)
			case superSet:
				supersetCount++
				//fmt.Printf("supersetCount:%v\n", supersetCount)
			}
			if subsetCount > 0 && supersetCount > 0 {
				return true
			}
			if cap(rels) > 0 {
				rels = append(rels, rel)
			}
			return false
		})
		ret = emptySet
		if !quit {
			// fmt.Printf("fullCount:%v\n", fullCount)
			// fmt.Printf("supersetCount:%v\n", supersetCount)
			// fmt.Printf("subsetCount:%v\n", subsetCount)

			// fmt.Printf(" greater.Len():%v\n", greater.Len())
			// fmt.Printf(" sparser.Len():%v\n", sparser.Len())
			if greater.Len() == fullCount {
				ret = fullSet
			} else if sparser.Len() == fullCount+subsetCount {
				ret = subSet
			} else if greater.Len() == fullCount+supersetCount {
				ret = superSet
			} else {
				ret = emptySet
			}
		}
		//fmt.Printf(" findRelations:%v\n", ret)
		return
	}

	if jm1.Len() >= jm2.Len() { // jm1 is a potential superset of jm2
		return findRelations(jm1, jm2)
	} else if jm1.Len() < jm2.Len() {
		ret, rels := findRelations(jm2, jm1)
		for i := range rels {
			rels[i].super, rels[i].sub = rels[i].sub, rels[i].super
			rels[i].inclusion.Flip()
		}
		ret.Flip()
		return ret, rels
	}

	return emptySet, nil
}
*/