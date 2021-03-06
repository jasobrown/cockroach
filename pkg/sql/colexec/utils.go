// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

var (
	zeroBoolColumn   = make([]bool, coldata.MaxBatchSize)
	zeroIntColumn    = make([]int, coldata.MaxBatchSize)
	zeroUint64Column = make([]uint64, coldata.MaxBatchSize)

	zeroDecimalValue  apd.Decimal
	zeroFloat64Value  float64
	zeroInt64Value    int64
	zeroIntervalValue duration.Duration
	zeroBytesValue    []byte
)

// overloadHelper is a utility struct that helps us avoid allocations
// of temporary decimals on every overloaded operation with them as well as
// plumbs through other useful information. In order for the templates to see
// it correctly, a local variable named `_overloadHelper` of this type must be declared
// before the inlined overloaded code.
type overloadHelper struct {
	tmpDec1, tmpDec2 apd.Decimal
	binFn            tree.TwoArgFn
	evalCtx          *tree.EvalContext
}

// makeWindowIntoBatch updates windowedBatch so that it provides a "window"
// into inputBatch starting at tuple index startIdx. It handles selection
// vectors on inputBatch as well (in which case windowedBatch will also have a
// "windowed" selection vector).
func makeWindowIntoBatch(
	windowedBatch, inputBatch coldata.Batch, startIdx int, inputTypes []*types.T,
) {
	inputBatchLen := inputBatch.Length()
	windowStart := startIdx
	windowEnd := inputBatchLen
	if sel := inputBatch.Selection(); sel != nil {
		// We have a selection vector on the input batch, and in order to avoid
		// deselecting (i.e. moving the data over), we will provide an adjusted
		// selection vector to the windowed batch as well.
		windowedBatch.SetSelection(true)
		windowIntoSel := sel[startIdx:inputBatchLen]
		copy(windowedBatch.Selection(), windowIntoSel)
		maxSelIdx := 0
		for _, selIdx := range windowIntoSel {
			if selIdx > maxSelIdx {
				maxSelIdx = selIdx
			}
		}
		windowStart = 0
		windowEnd = maxSelIdx + 1
	} else {
		windowedBatch.SetSelection(false)
	}
	for i := range inputTypes {
		window := inputBatch.ColVec(i).Window(windowStart, windowEnd)
		windowedBatch.ReplaceCol(window, i)
	}
	windowedBatch.SetLength(inputBatchLen - startIdx)
}

func newPartitionerToOperator(
	allocator *colmem.Allocator,
	types []*types.T,
	partitioner colcontainer.PartitionedQueue,
	partitionIdx int,
) *partitionerToOperator {
	return &partitionerToOperator{
		partitioner:  partitioner,
		partitionIdx: partitionIdx,
		batch:        allocator.NewMemBatchWithFixedCapacity(types, coldata.BatchSize()),
	}
}

// partitionerToOperator is an Operator that Dequeue's from the corresponding
// partition on every call to Next. It is a converter from filled in
// PartitionedQueue to Operator.
type partitionerToOperator struct {
	colexecbase.ZeroInputNode
	NonExplainable

	partitioner  colcontainer.PartitionedQueue
	partitionIdx int
	batch        coldata.Batch
}

var _ colexecbase.Operator = &partitionerToOperator{}

func (p *partitionerToOperator) Init() {}

func (p *partitionerToOperator) Next(ctx context.Context) coldata.Batch {
	if err := p.partitioner.Dequeue(ctx, p.partitionIdx, p.batch); err != nil {
		colexecerror.InternalError(err)
	}
	return p.batch
}

// newAppendOnlyBufferedBatch returns a new appendOnlyBufferedBatch that has
// initial zero capacity and could grow arbitrarily large with append() method.
// It is intended to be used by the operators that need to buffer unknown
// number of tuples.
// colsToStore indicates the positions of columns to actually store in this
// batch. All columns are stored if colsToStore is nil, but when it is non-nil,
// then columns with positions not present in colsToStore will remain
// zero-capacity vectors.
// TODO(yuzefovich): consider whether it is beneficial to start out with
// non-zero capacity.
func newAppendOnlyBufferedBatch(
	allocator *colmem.Allocator, typs []*types.T, colsToStore []int,
) *appendOnlyBufferedBatch {
	if colsToStore == nil {
		colsToStore = make([]int, len(typs))
		for i := range colsToStore {
			colsToStore[i] = i
		}
	}
	batch := allocator.NewMemBatchWithFixedCapacity(typs, 0 /* capacity */)
	return &appendOnlyBufferedBatch{
		Batch:       batch,
		colVecs:     batch.ColVecs(),
		colsToStore: colsToStore,
	}
}

// appendOnlyBufferedBatch is a wrapper around coldata.Batch that should be
// used by operators that buffer many tuples into a single batch by appending
// to it. It stores the length of the batch separately and intercepts calls to
// Length() and SetLength() in order to avoid updating offsets on vectors of
// types.Bytes type - which would result in a quadratic behavior - because
// it is not necessary since coldata.Vec.Append maintains the correct offsets.
//
// Note: "appendOnly" in the name indicates that the tuples should *only* be
// appended to the vectors (which can be done via explicit Vec.Append calls or
// using utility append() method); however, this batch prohibits appending and
// replacing of the vectors themselves.
type appendOnlyBufferedBatch struct {
	coldata.Batch

	length      int
	colVecs     []coldata.Vec
	colsToStore []int
}

var _ coldata.Batch = &appendOnlyBufferedBatch{}

func (b *appendOnlyBufferedBatch) Length() int {
	return b.length
}

func (b *appendOnlyBufferedBatch) SetLength(n int) {
	b.length = n
}

func (b *appendOnlyBufferedBatch) ColVec(i int) coldata.Vec {
	return b.colVecs[i]
}

func (b *appendOnlyBufferedBatch) ColVecs() []coldata.Vec {
	return b.colVecs
}

func (b *appendOnlyBufferedBatch) AppendCol(coldata.Vec) {
	colexecerror.InternalError("AppendCol is prohibited on appendOnlyBufferedBatch")
}

func (b *appendOnlyBufferedBatch) ReplaceCol(coldata.Vec, int) {
	colexecerror.InternalError("ReplaceCol is prohibited on appendOnlyBufferedBatch")
}

// append is a helper method that appends all tuples with indices in range
// [startIdx, endIdx) from batch (paying attention to the selection vector)
// into b.
// NOTE: this does *not* perform memory accounting.
func (b *appendOnlyBufferedBatch) append(batch coldata.Batch, startIdx, endIdx int) {
	for _, colIdx := range b.colsToStore {
		b.colVecs[colIdx].Append(
			coldata.SliceArgs{
				Src:         batch.ColVec(colIdx),
				Sel:         batch.Selection(),
				DestIdx:     b.length,
				SrcStartIdx: startIdx,
				SrcEndIdx:   endIdx,
			},
		)
	}
	b.length += endIdx - startIdx
}

// maybeAllocate* methods make sure that the passed in array is allocated, of
// the desired length and zeroed out.
func maybeAllocateUint64Array(array []uint64, length int) []uint64 {
	if cap(array) < length {
		return make([]uint64, length)
	}
	array = array[:length]
	for n := 0; n < length; n += copy(array[n:], zeroUint64Column) {
	}
	return array
}

func maybeAllocateBoolArray(array []bool, length int) []bool {
	if cap(array) < length {
		return make([]bool, length)
	}
	array = array[:length]
	for n := 0; n < length; n += copy(array[n:], zeroBoolColumn) {
	}
	return array
}

// maybeAllocateLimitedUint64Array is an optimized version of
// maybeAllocateUint64Array that can *only* be used when length is at most
// coldata.MaxBatchSize.
func maybeAllocateLimitedUint64Array(array []uint64, length int) []uint64 {
	if cap(array) < length {
		return make([]uint64, length)
	}
	array = array[:length]
	copy(array, zeroUint64Column)
	return array
}

// maybeAllocateLimitedBoolArray is an optimized version of
// maybeAllocateBool64Array that can *only* be used when length is at most
// coldata.MaxBatchSize.
func maybeAllocateLimitedBoolArray(array []bool, length int) []bool {
	if cap(array) < length {
		return make([]bool, length)
	}
	array = array[:length]
	copy(array, zeroBoolColumn)
	return array
}
