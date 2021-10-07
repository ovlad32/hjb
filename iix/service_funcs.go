package iix

import (
	"context"

	"github.com/ovlad32/hjb/meta"
	"github.com/pkg/errors"
)

func resolveExtentsFromValues(ctx context.Context, imt iMetaProvider, norm iNormalizer, originalValues []string) (
	columns []*meta.Column, values []string, rejected []*meta.Column, err error) {
	//TODO: implement rejected
	if len(originalValues) == 0 {
		err = errors.Errorf("No values")
		return
	}
	originalColumns, err := imt.Columns(ctx)
	cloned := false
	totalOriginalColumns := len(originalColumns)

	if err != nil {
		err = errors.Wrap(err, "couldn't get columns details")
		return
	}
	if totalOriginalColumns < len(originalValues) {
		err = errors.Errorf("Number of columns is less than values provided; %v < %v", len(originalColumns), len(originalValues))
		return
	}
	vIndex := -1
	for colIndex := 0; colIndex < totalOriginalColumns; colIndex++ {
		splitValues, validFlags, erre := norm.Normalize(ctx, originalColumns[colIndex], originalValues[colIndex])
		if erre != nil {
			err = errors.Wrapf(erre,
				"Couldn't normalize value %v for column %v",
				originalValues[colIndex], originalColumns[colIndex],
			)
			return
		}

		// splitValues can be empty if no transformation happened
		if values == nil && len(splitValues) <= 1 {
			if len(validFlags) == 1 && !validFlags[0] {
				continue
			}
			vIndex++
			if len(splitValues) == 1 {
				originalValues[vIndex] = splitValues[0]
			}
			if vIndex < colIndex {
				if !cloned {
					{
						temp := make([]*meta.Column, totalOriginalColumns)
						copy(temp, originalColumns)
						originalColumns = temp
					}
					{
						temp := make([]string, len(originalValues))
						copy(temp, originalValues)
						originalValues = temp
					}
					cloned = true
				}
				originalColumns[vIndex] = originalColumns[colIndex]
			}
			continue
		}
		var extents meta.Columns
		if len(splitValues) > 1 {
			var validFlag bool
			for _, validFlag = range validFlags {
				if !validFlag {
					break
				}
			}
			if !validFlag {
				continue
			}
			if values == nil {
				vIndex++
				columns = make([]*meta.Column, vIndex, len(originalColumns))
				values = make([]string, vIndex, len(originalColumns))
				copy(columns, originalColumns[0:vIndex])
				copy(values, originalValues[0:vIndex])
			}
			extents, erre = imt.CreateExtentsWithCount(ctx, originalColumns[colIndex], len(splitValues))
			if erre != nil {
				err = errors.Wrapf(erre,
					"Couldn't create column extensions with column value value %v for column %v  ",
					originalValues[colIndex], originalColumns[colIndex],
				)
				return
			}

		}
		for j := range splitValues {
			if len(splitValues) == 1 {
				if validFlags[j] {
					columns = append(columns, originalColumns[colIndex])
					values = append(values, splitValues[j])
				}
			} else {
				if validFlags[j] {
					columns = append(columns, extents[j])
					values = append(values, splitValues[j])
				}
			}
		}
	}
	if values == nil {
		columns = originalColumns[0 : vIndex+1]
		values = originalValues[0 : vIndex+1]
	}
	return
}

func groupByValue(ctx context.Context, columns []*meta.Column, values []string) (result map[string]meta.Columns, err error) {
	if len(columns) == 0 {
		err = errors.Errorf("List of defined columns is empty!")
		return
	}
	if len(columns) != len(values) {
		err = errors.Errorf("Number of column and number of values don't match: %v != %v", len(columns), len(values))
		return
	}
	result = make(map[string]meta.Columns)
	for i := range columns {
		var sameValueOf meta.Columns
		var found bool
		if sameValueOf, found = result[values[i]]; !found {
			sameValueOf = make(meta.Columns, 0, len(values))
		}
		result[values[i]] = append(sameValueOf, columns[i])
	}
	return
}
