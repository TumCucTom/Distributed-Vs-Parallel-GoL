package stubs

import (
	"net/rpc"
	"uk.ac.bris.cs/gameoflife/util"
)

var CalculateWorldSegment = "Node.GetSegment"
var CalculateWorldSegmentParallel = "ParallelNode.GetSegment"
var PartialRows = "ParallelNode.AcceptPartialRows"
var Connect = "ParallelNode.ConnectToNextWorker"
var End = "ParallelNode.Quit"

type WorkerRequest struct {
	WholeWorld [][]uint8
	Start      int
	End        int
	Size       int
	LowerBound [][]uint8
	UpperBound [][]uint8
	Worker     *rpc.Client
}

type WorkerResponse struct {
	Segment    [][]uint8
	Flipped    []util.Cell
	LowerBound []uint8
	UpperBound []uint8
}

type ParallelWorkerRequest struct {
	WholeWorld [][]uint8
	Start      int
	End        int
	Size       int
	Threads    int
}

type ParallelWorkerResponse struct {
	Segment [][]uint8
	Flipped []util.Cell
}
type HaloRowsReq struct {
	Row []int
}

type HaloRowsRes struct {
	Row []int
}

type AddressReq struct {
	Address string
}
