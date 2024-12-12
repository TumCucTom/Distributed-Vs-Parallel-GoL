package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"uk.ac.bris.cs/gameoflife/stubs"
)

var nextWorker *rpc.Client

var lowerBoundGive, lowerBoundGet []int

var wg, calculatingLowerBound, receivingLowerBound sync.WaitGroup

type ParallelNode struct{}

type pixel struct {
	X     int
	Y     int
	Value uint8
}

var quitting = make(chan bool, 1)

func (n *ParallelNode) GetSegment(req stubs.ParallelWorkerRequest, res *stubs.ParallelWorkerResponse) error {
	// start processing GoL turn
	res.Segment = calculateNewAliveParallel(req.Threads, req.WholeWorld, req.Start, req.End, req.Size)
	return nil
}

func (n *ParallelNode) Quit(_ stubs.WorkerRequest, _ *stubs.WorkerResponse) error {
	// quit gracefully
	quitting <- true
	return nil
}

func (n *ParallelNode) ConnectToNextWorker(req stubs.AddressReq, _ *stubs.EmptyRes) error {
	// connect to the next worker in the process for halo
	worker, err := rpc.Dial("tcp", req.Address)
	if err != nil {
		return err
	}
	nextWorker = worker
	return nil
}

func (n *ParallelNode) AcceptPartialRows(req *stubs.HaloRowsReq, res *stubs.HaloRowsRes) error {
	// accept the lower row as a req
	lowerBoundGet = req.Row

	// once lower row is processed, send over as res
	receivingLowerBound.Done()
	calculatingLowerBound.Wait()
	calculatingLowerBound.Add(1)
	res.Row = lowerBoundGive
	return nil
}

func giveRowsWorker(partialRow []int) []int {
	// give rows as a req to next node
	res := new(stubs.HaloRowsRes)
	req := stubs.HaloRowsReq{
		Row: partialRow,
	}
	if err := nextWorker.Call(stubs.PartialRows, &req, res); err != nil {
		fmt.Println(err)
	}
	// receive your needed row as resp
	return res.Row
}

func calculateNewAliveParallel(workerNum int, world [][]uint8, start, end, worldSize int) [][]uint8 {
	//create channels for the different parallel segments to communicate their changes

	splitSegments := make([]chan pixel, workerNum)
	for i := range splitSegments {
		splitSegments[i] = make(chan pixel, worldSize*worldSize)
	}

	// start workers to make the world
	setupWorkers(worldSize, start, end, workerNum, splitSegments, world, start == 0, end == worldSize)

	//var cells []util.Cell

	// wait for workers to finish
	wg.Wait()

	// combine changes
	for i := 0; i < workerNum; i++ {
		length := len(splitSegments[i])
		for j := 0; j < length; j++ {
			item := <-splitSegments[i]
			world[item.X][item.Y] = item.Value
		}
	}

	// return your section of the world
	return world[start:end]
}
func setupWorkers(worldHeight, sectionStart, sectionEnd, workerNum int, splitSegments []chan pixel, world [][]uint8, starting, ending bool) {
	// ge the number of rows to be processed
	sectionHeight := sectionEnd - sectionStart
	numRows := sectionHeight / workerNum

	// creete channels for inter node - parallel halo
	haloTake := make([]chan []int, workerNum-1)
	for i := range haloTake {
		haloTake[i] = make(chan []int, 1)
	}

	haloGive := make([]chan []int, workerNum-1)
	for i := range haloGive {
		haloGive[i] = make(chan []int, 1)
	}

	// you are one worker calculating the entire world
	if starting && ending {
		wg.Add(1)
		// no halo needed
		go runWorkerNoHalo(worldHeight, 0, sectionHeight, splitSegments[0], world, true, true)
		return
	}

	if workerNum == 1 {
		wg.Add(1)
		// no inner halo needed
		go runWorkerHaloBoth(worldHeight, sectionStart, sectionStart+numRows, splitSegments[0], world, starting, ending)
		return
	}

	// halo behind a node
	wg.Add(1)
	go runWorkerHaloStart(worldHeight, sectionStart, sectionStart+numRows, splitSegments[0], world, starting, false, haloTake[0], haloGive[0])

	// no halo
	i := 1
	for i < workerNum-1 {
		wg.Add(1)
		go runWorker(worldHeight, sectionStart+i*numRows, sectionStart+(i+1)*numRows, splitSegments[i], world, false, false, haloTake[i-1], haloGive[i-1], haloTake[i], haloGive[i])
		i++
	}

	// halo ahead a node
	if workerNum > 1 {
		// final worker does the remaining rows
		wg.Add(1)
		go runWorkerHaloEnd(worldHeight, sectionStart+i*numRows, sectionStart+sectionHeight, splitSegments[i], world, false, ending, haloTake[i-1], haloGive[i-1])
	}
}

// go routine for a worker with no external halo
func runWorkerNoHalo(worldHeight, start, end int, splitSegment chan pixel, world [][]uint8, starting, ending bool) {
	defer wg.Done()
	neighboursWorld := calculateNeighboursNone(start, end, worldHeight, world, starting, ending)
	calculateNextWorld(start, end, worldHeight, splitSegment, world, neighboursWorld)
}

// go routine for a worker with no external halo
func runWorker(worldHeight, start, end int, splitSegment chan pixel, world [][]uint8, starting, ending bool, lowerTook, lowerGave, takeUpper, giveUpper chan []int) {
	defer wg.Done()
	// get the number of neighbours for relevant part
	neighboursWorld := calculateNeighbours(start, end, worldHeight, world, starting, ending, lowerTook, lowerGave, takeUpper, giveUpper)
	// now calculate the changes to the board
	calculateNextWorld(start, end, worldHeight, splitSegment, world, neighboursWorld)
}

// go routine for a worker with halo on both side but no internal
func runWorkerHaloBoth(worldHeight, start, end int, splitSegment chan pixel, world [][]uint8, starting, ending bool) {
	defer wg.Done()
	// get the number of neighbours for relevant part
	neighboursWorld := calculateNeighboursHaloBoth(start, end, worldHeight, world, starting, ending)
	// now calculate the changes to the board
	calculateNextWorld(start, end, worldHeight, splitSegment, world, neighboursWorld)
}

// go routine for a worker with halo behind
func runWorkerHaloStart(worldHeight, start, end int, splitSegment chan pixel, world [][]uint8, starting, ending bool, takeU, giveU chan []int) {
	defer wg.Done()
	// get the number of neighbours for relevant part
	neighboursWorld := calculateNeighboursHaloStart(start, end, worldHeight, world, starting, ending, takeU, giveU)
	// now calculate the changes to the board
	calculateNextWorld(start, end, worldHeight, splitSegment, world, neighboursWorld)
}

// go routine for a worker with halo ahead
func runWorkerHaloEnd(worldHeight, start, end int, splitSegment chan pixel, world [][]uint8, starting, ending bool, lowerTook, lowerGave chan []int) {
	defer wg.Done()
	// get the number of neighbours for relevant part
	neighboursWorld := calculateNeighboursHaloEnd(start, end, worldHeight, world, starting, ending, lowerTook, lowerGave)
	// now calculate the changes to the board
	calculateNextWorld(start, end, worldHeight, splitSegment, world, neighboursWorld)
}

func calculateNextWorld(start, end, width int, c chan pixel, world [][]uint8, neighboursWorld [][]int) {
	//iterate through your section of the board
	for y := start; y < end; y++ {
		for x := 0; x < width; x++ {
			neighbors := neighboursWorld[y][x]
			// become dead from alive
			if (neighbors < 2 || neighbors > 3) && world[y][x] == 255 {
				c <- pixel{y, x, 0}
			} else if neighbors == 3 && world[y][x] == 0 {
				// become alive from dead
				c <- pixel{y, x, 255}
			}
		}
	}
}
func calculateNeighboursNone(start, end, width int, world [][]uint8, starting, ending bool) [][]int {
	neighbours := make([][]int, width)
	for i := range neighbours {
		neighbours[i] = make([]int, width)
	}

	// calculate wrap around values for workers starting on a top or bottom edge
	if !(starting && ending) {
		if starting {
			for x := 0; x < width; x++ {
				if world[width-1][x] == 255 {
					for i := -1; i <= 1; i++ {
						//for image wrap around
						xCoord := x + i
						if xCoord < 0 {
							xCoord = width - 1
						} else if xCoord >= width {
							xCoord = 0
						}
						neighbours[0][xCoord]++
					}
				}
			}
		} else {
			start--
		}

		if ending {
			for x := 0; x < width; x++ {
				if world[0][x] == 255 {
					for i := -1; i <= 1; i++ {
						//for image wrap around
						xCoord := x + i
						if xCoord < 0 {
							xCoord = width - 1
						} else if xCoord >= width {
							xCoord = 0
						}
						neighbours[width-1][xCoord]++
					}
				}
			}
		} else {
			end++
		}
	}

	// for your range, calculate the workers
	for y := start; y < end; y++ {
		for x := 0; x < width; x++ {
			// if a cell is 255
			if world[y][x] == 255 {
				// add 1 to all neighbours
				// i and j are the offset
				for i := -1; i <= 1; i++ {
					for j := -1; j <= 1; j++ {

						// if you are not offset, do not add one. This is yourself
						if !(i == 0 && j == 0) {
							ny, nx := y+i, x+j
							if nx < 0 {
								nx = width - 1
							} else if nx == width {
								nx = 0
							} else {
								nx = nx % width
							}

							if ny < 0 {
								ny = width - 1
							} else if ny == width {
								ny = 0
							} else {
								ny = ny % width
							}

							neighbours[ny][nx]++
						}

					}
				}
			}
		}
	}
	return neighbours
}
func calculateNeighbours(start, end, width int, world [][]uint8, starting, ending bool, lowerTook, lowerGave, takeUpper, giveUpper chan []int) [][]int {
	neighbours := make([][]int, width)
	for i := range neighbours {
		neighbours[i] = make([]int, width)
	}

	// calculate the lowest row
	lowerBoundGive = make([]int, width)
	for x := 0; x < width; x++ {
		// if a cell is 255
		if world[start][x] == 255 {
			// add 1 to all neighbours
			// i and j are the offset
			for i := -1; i <= 1; i++ {
				for j := -1; j <= 1; j++ {

					// if you are not offset, do not add one. This is yourself
					if !(i == 0 && j == 0) {
						ny, nx := start+i, x+j
						if nx < 0 {
							nx = width - 1
						} else if nx == width {
							nx = 0
						} else {
							nx = nx % width
						}

						if ny < 0 {
							ny = width - 1
						} else if ny == width {
							ny = 0

						} else {
							ny = ny % width
						}

						neighbours[ny][nx]++
						if i == -1 {
							lowerBoundGive[nx]++
						}
					}

				}
			}
		}
	}
	// give additional data to neighbours with halo
	lowerTook <- lowerBoundGive

	// calculate upmost row
	upper := make([]int, width)
	for x := 0; x < width; x++ {
		// if a cell is 255
		if world[end-1][x] == 255 {
			// add 1 to all neighbours
			// i and j are the offset
			for i := -1; i <= 1; i++ {
				for j := -1; j <= 1; j++ {

					// if you are not offset, do not add one. This is yourself
					if !(i == 0 && j == 0) {
						ny, nx := end-1+i, x+j
						if nx < 0 {
							nx = width - 1
						} else if nx == width {
							nx = 0
						} else {
							nx = nx % width
						}

						if ny < 0 {
							ny = width - 1
						} else if ny == width {
							ny = 0
						} else {
							ny = ny % width
						}

						neighbours[ny][nx]++
						if i == 1 {
							upper[nx]++
						}
					}

				}
			}
		}
	}
	// give additionla info to neighbours with halo
	giveUpper <- upper

	// calculate neighbours for you area
	for y := start; y < end; y++ {
		for x := 0; x < width; x++ {
			// if a cell is 255
			if world[y][x] == 255 {
				// add 1 to all neighbours
				// i and j are the offset
				for i := -1; i <= 1; i++ {
					for j := -1; j <= 1; j++ {

						// if you are not offset, do not add one. This is yourself
						if !(i == 0 && j == 0) {
							ny, nx := y+i, x+j
							if nx < 0 {
								nx = width - 1
							} else if nx == width {
								nx = 0
							} else {
								nx = nx % width
							}

							if ny < 0 {
								ny = width - 1
							} else if ny == width {
								ny = 0
							} else {
								ny = ny % width
							}

							neighbours[ny][nx]++
						}

					}
				}
			}
		}
	}
	// take extra info on upper row
	extra := <-takeUpper
	for i, item := range extra {
		neighbours[end-1][i] += item
	}
	// take extra info on lower row
	extra = <-lowerGave
	for i, item := range extra {
		neighbours[start][i] += item
	}

	return neighbours
}

func calculateNeighboursHaloBoth(start, end, width int, world [][]uint8, starting, ending bool) [][]int {
	neighbours := make([][]int, width)
	for i := range neighbours {
		neighbours[i] = make([]int, width)
	}

	//calcualte lowest row
	lowerBoundGive = make([]int, width)
	for x := 0; x < width; x++ {
		// if a cell is 255
		if world[start][x] == 255 {
			// add 1 to all neighbours
			// i and j are the offset
			for i := -1; i <= 1; i++ {
				for j := -1; j <= 1; j++ {

					// if you are not offset, do not add one. This is yourself
					if !(i == 0 && j == 0) {
						ny, nx := start+i, x+j
						if nx < 0 {
							nx = width - 1
						} else if nx == width {
							nx = 0
						} else {
							nx = nx % width
						}

						if ny < 0 {
							ny = width - 1
						} else if ny == width {
							ny = 0

						} else {
							ny = ny % width
						}

						neighbours[ny][nx]++
						if i == -1 {
							lowerBoundGive[nx]++
						}
					}

				}
			}
		}
	}
	//give additional info via halo
	calculatingLowerBound.Done()

	// calculate upmost row
	upper := make([]int, width)
	for x := 0; x < width; x++ {
		// if a cell is 255
		if world[end-1][x] == 255 {
			// add 1 to all neighbours
			// i and j are the offset
			for i := -1; i <= 1; i++ {
				for j := -1; j <= 1; j++ {

					// if you are not offset, do not add one. This is yourself
					if !(i == 0 && j == 0) {
						ny, nx := end-1+i, x+j
						if nx < 0 {
							nx = width - 1
						} else if nx == width {
							nx = 0
						} else {
							nx = nx % width
						}

						if ny < 0 {
							ny = width - 1
						} else if ny == width {
							ny = 0
						} else {
							ny = ny % width
						}

						neighbours[ny][nx]++
						if i == 1 {
							upper[nx]++
						}
					}

				}
			}
		}
	}
	// give and request extra info via halo
	extra := giveRowsWorker(upper)
	for i, item := range extra {
		neighbours[end-1][i] += item
	}

	// process neighbours
	for y := start + 1; y < end-1; y++ {
		for x := 0; x < width; x++ {
			// if a cell is 255
			if world[y][x] == 255 {
				// add 1 to all neighbours
				// i and j are the offset
				for i := -1; i <= 1; i++ {
					for j := -1; j <= 1; j++ {

						// if you are not offset, do not add one. This is yourself
						if !(i == 0 && j == 0) {
							ny, nx := y+i, x+j
							if nx < 0 {
								nx = width - 1
							} else if nx == width {
								nx = 0
							} else {
								nx = nx % width
							}

							if ny < 0 {
								ny = width - 1
							} else if ny == width {
								ny = 0
							} else {
								ny = ny % width
							}

							neighbours[ny][nx]++
						}

					}
				}
			}
		}
	}

	// update lowest row using halo
	receivingLowerBound.Wait()
	receivingLowerBound.Add(1)
	for i, item := range lowerBoundGet {
		neighbours[start][i] += item
	}
	return neighbours
}

func calculateNeighboursHaloStart(start, end, width int, world [][]uint8, starting, ending bool, take, give chan []int) [][]int {
	neighbours := make([][]int, width)
	for i := range neighbours {
		neighbours[i] = make([]int, width)
	}

	// calculate lowest row
	lowerBoundGive = make([]int, width)
	for x := 0; x < width; x++ {
		// if a cell is 255
		if world[start][x] == 255 {
			// add 1 to all neighbours
			// i and j are the offset
			for i := -1; i <= 1; i++ {
				for j := -1; j <= 1; j++ {

					// if you are not offset, do not add one. This is yourself
					if !(i == 0 && j == 0) {
						ny, nx := start+i, x+j
						if nx < 0 {
							nx = width - 1
						} else if nx == width {
							nx = 0
						} else {
							nx = nx % width
						}

						if ny < 0 {
							ny = width - 1
						} else if ny == width {
							ny = 0

						} else {
							ny = ny % width
						}

						neighbours[ny][nx]++
						if i == -1 {
							lowerBoundGive[nx]++
						}
					}

				}
			}
		}
	}
	// give additional info
	calculatingLowerBound.Done()

	//calculate uppermost row
	upper := make([]int, width)
	for x := 0; x < width; x++ {
		// if a cell is 255
		if world[end-1][x] == 255 {
			// add 1 to all neighbours
			// i and j are the offset
			for i := -1; i <= 1; i++ {
				for j := -1; j <= 1; j++ {

					// if you are not offset, do not add one. This is yourself
					if !(i == 0 && j == 0) {
						ny, nx := end-1+i, x+j
						if nx < 0 {
							nx = width - 1
						} else if nx == width {
							nx = 0
						} else {
							nx = nx % width
						}

						if ny < 0 {
							ny = width - 1
						} else if ny == width {
							ny = 0
						} else {
							ny = ny % width
						}

						neighbours[ny][nx]++
						if i == 1 {
							upper[nx]++
						}
					}

				}
			}
		}
	}
	//give additional info
	give <- upper

	// calculaute neighbours
	for y := start + 1; y < end-1; y++ {
		for x := 0; x < width; x++ {
			// if a cell is 255
			if world[y][x] == 255 {
				// add 1 to all neighbours
				// i and j are the offset
				for i := -1; i <= 1; i++ {
					for j := -1; j <= 1; j++ {

						// if you are not offset, do not add one. This is yourself
						if !(i == 0 && j == 0) {
							ny, nx := y+i, x+j
							if nx < 0 {
								nx = width - 1
							} else if nx == width {
								nx = 0
							} else {
								nx = nx % width
							}

							if ny < 0 {
								ny = width - 1
							} else if ny == width {
								ny = 0
							} else {
								ny = ny % width
							}

							neighbours[ny][nx]++
						}

					}
				}
			}
		}
	}

	// get additional lowerbound info
	receivingLowerBound.Wait()
	receivingLowerBound.Add(1)
	for i, item := range lowerBoundGet {
		neighbours[start][i] += item
	}
	// take upperbound info
	extra := <-take
	for i, item := range extra {
		neighbours[end-1][i] += item
	}

	return neighbours
}

func calculateNeighboursHaloEnd(start, end, width int, world [][]uint8, starting, ending bool, lowerTook, lowerGave chan []int) [][]int {
	neighbours := make([][]int, width)
	for i := range neighbours {
		neighbours[i] = make([]int, width)
	}

	// calculate uppermost info
	upper := make([]int, width)
	for x := 0; x < width; x++ {
		// if a cell is 255
		if world[end-1][x] == 255 {
			// add 1 to all neighbours
			// i and j are the offset
			for i := -1; i <= 1; i++ {
				for j := -1; j <= 1; j++ {

					// if you are not offset, do not add one. This is yourself
					if !(i == 0 && j == 0) {
						ny, nx := end-1+i, x+j
						if nx < 0 {
							nx = width - 1
						} else if nx == width {
							nx = 0
						} else {
							nx = nx % width
						}

						if ny < 0 {
							ny = width - 1
						} else if ny == width {
							ny = 0
						} else {
							ny = ny % width
						}

						neighbours[ny][nx]++
						if i == 1 {
							upper[nx]++
						}
					}

				}
			}
		}
	}
	// give and request uppermost info
	extra := giveRowsWorker(upper)
	for i, item := range extra {
		neighbours[end-1][i] += item
	}

	// process lowermost info
	lowerBoundGive = make([]int, width)
	for x := 0; x < width; x++ {
		// if a cell is 255
		if world[start][x] == 255 {
			// add 1 to all neighbours
			// i and j are the offset
			for i := -1; i <= 1; i++ {
				for j := -1; j <= 1; j++ {

					// if you are not offset, do not add one. This is yourself
					if !(i == 0 && j == 0) {
						ny, nx := start+i, x+j
						if nx < 0 {
							nx = width - 1
						} else if nx == width {
							nx = 0
						} else {
							nx = nx % width
						}

						if ny < 0 {
							ny = width - 1
						} else if ny == width {
							ny = 0

						} else {
							ny = ny % width
						}

						neighbours[ny][nx]++
						if i == -1 {
							lowerBoundGive[nx]++
						}
					}

				}
			}
		}
	}
	//give lowerbound info
	lowerTook <- lowerBoundGive

	//process neighbours
	for y := start + 1; y < end-1; y++ {
		for x := 0; x < width; x++ {
			// if a cell is 255
			if world[y][x] == 255 {
				// add 1 to all neighbours
				// i and j are the offset
				for i := -1; i <= 1; i++ {
					for j := -1; j <= 1; j++ {

						// if you are not offset, do not add one. This is yourself
						if !(i == 0 && j == 0) {
							ny, nx := y+i, x+j
							if nx < 0 {
								nx = width - 1
							} else if nx == width {
								nx = 0
							} else {
								nx = nx % width
							}

							if ny < 0 {
								ny = width - 1
							} else if ny == width {
								ny = 0
							} else {
								ny = ny % width
							}

							neighbours[ny][nx]++
						}

					}
				}
			}
		}
	}
	// get additional info
	extra = <-lowerGave
	for i, item := range extra {
		neighbours[start][i] += item
	}
	return neighbours
}

func main() {
	// listen for requests from server or nodes
	serverPort := flag.String("port", "8040", "Port to Listen")
	flag.Parse()

	// set waiting for calculations to occur
	receivingLowerBound.Add(1)
	calculatingLowerBound.Add(1)

	// listen for requests from server or nodes
	rpc.Register(&ParallelNode{})
	listener, err := net.Listen("tcp", "127.0.0.1:"+*serverPort)
	if err != nil {
		log.Fatal("Listener error:", err)
	}
	log.Println("Server listening on port", *serverPort)
	defer listener.Close()
	go rpc.Accept(listener)

	_ = <-quitting
}
