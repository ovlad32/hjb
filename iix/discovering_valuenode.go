package iix

import (
	"github.com/ovlad32/hjb/iixs"
	"github.com/ovlad32/hjb/meta"
)

type valueNode struct {
	lakeColumns []*meta.Column
	value       string
	rcs         iixs.Rcs
}

func (n valueNode) Columns() []*meta.Column {
	return n.lakeColumns
}
func (n valueNode) ColumnsIDs() []string {
	ids := make([]string, 0, len(n.lakeColumns))
	for j := range n.lakeColumns {
		ids = append(ids, n.lakeColumns[j].ID.String)
	}
	return ids
}
