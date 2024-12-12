package gol

import (
	"fmt"
	"sync"
	"time"
	"uk.ac.bris.cs/gameoflife/util"
)

var wg, pause, executingKeyPress, pauseReached sync.WaitGroup
var turn IntContainer
var worldGlobal [][]uint8

type BoolContainer struct {
	mu      sync.Mutex
	boolean bool
}

type IntContainer struct {
	mu  sync.Mutex
	num int
}

type WorldContainer struct {
	mu    sync.Mutex
	world [][]uint8
}

func (c *IntContainer) inc() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.num++
}

func (c *IntContainer) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.num = 0
}

func (c *IntContainer) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.num
}

func (c *BoolContainer) setFalse() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.boolean = false
}

func (c *BoolContainer) setTrue() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.boolean = true
}

func (c *BoolContainer) get() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.boolean
}

var end, snapshot, getCount BoolContainer

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- [][]uint8
	ioInput    <-chan [][]uint8
	keyPresses <-chan rune
}

type pixel struct {
	X     int
	Y     int
	Value uint8
}

func getAliveCells(p Params, world [][]uint8) []util.Cell {
	// make the slice
	aliveCells := make([]util.Cell, 0)

	// check all cells and if alive append it
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			// append every 255 cells
			if world[x][y] == 255 {
				aliveCells = append(aliveCells, util.Cell{X: x, Y: y})
			}
		}
	}

	return aliveCells
}

func getNumAliveCells(p Params, world [][]uint8) int {
	num := 0

	// iterate through whole board
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			// add 1 for every alive cell
			if world[x][y] == 255 {
				num++
			}
		}
	}

	return num
}

func makeOutput(p Params, c distributorChannels, world [][]uint8) {

	// add a get output to the command channel
	c.ioCommand <- ioOutput
	// give filename
	c.ioFilename <- fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, p.Turns)

	// pass the world slice
	c.ioOutput <- world

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
}

func makeOutputTurnWithTurnNum(p Params, c distributorChannels, turns int, world [][]uint8) {

	// add a get output to the command channel
	c.ioCommand <- ioOutput
	// give filename with specified turns
	filename := fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, turns)
	c.ioFilename <- filename

	// pass the world slice
	c.ioOutput <- world

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
	c.events <- ImageOutputComplete{turns, filename}
}

func calculateNewAliveParallel(p Params, workerNum int, c distributorChannels, world [][]uint8) [][]uint8 {
	// create channels for the workers to pass back their data on
	splitSegments := make([]chan pixel, workerNum)
	for i := range splitSegments {
		splitSegments[i] = make(chan pixel, p.ImageWidth*p.ImageWidth)
	}

	// start workers to make the world
	setupWorkers(p.ImageHeight, workerNum, splitSegments, world)

	// setup cells for SDL flipping
	var cells []util.Cell

	// wait for workers to finish processing
	wg.Wait()

	// for every worker, use their changed cell data to flip cells and update board
	for i := 0; i < workerNum; i++ {
		length := len(splitSegments[i])
		for j := 0; j < length; j++ {
			item := <-splitSegments[i]
			world[item.X][item.Y] = item.Value
			cells = append(cells, util.Cell{X: item.X, Y: item.Y})
		}
	}

	// flip cells for SDL
	c.events <- CellsFlipped{turn.get() + 1, cells}

	// return the new state of the board
	return world
}
func setupWorkers(size, workerNum int, splitSegments []chan pixel, world [][]uint8) {
	// the number of rows that the first n-1 workers should calculate
	numRows := size / workerNum

	// run the first n-1 workers for their given rows
	i := 0
	for i < workerNum-1 {
		wg.Add(1)
		go runWorker(size, i*numRows, (i+1)*numRows, splitSegments[i], world)
		i++
	}

	// final worker does the remaining rows
	wg.Add(1)
	go runWorker(size, i*numRows, size, splitSegments[i], world)
}
func runWorker(size, start, end int, splitSegment chan pixel, world [][]uint8) {
	// state you are done when returned from this function
	defer wg.Done()

	// add relevant changes to worker specific channel
	calculateNextWorld(start, end, size, world, splitSegment)
}

func calculateNextWorld(start, end, width int, world [][]uint8, c chan pixel) {
	// calculate relevant neighbours
	neighboursWorld := calculateNeighbours(start, end, width, world)

	// for all cells in this workers range
	// send an update if one is needed according to GoL rules
	for y := start; y < end; y++ {
		for x := 0; x < width; x++ {
			neighbors := neighboursWorld[y][x]
			if (neighbors < 2 || neighbors > 3) && world[y][x] == 255 {
				// if an alive cell becomes dead
				c <- pixel{y, x, 0}
			} else if neighbors == 3 && world[y][x] == 0 {
				// if a dead cell becomes alive
				c <- pixel{y, x, 255}
			}
		}
	}
}

func calculateNeighbours(start, end, width int, world [][]uint8) [][]int {
	//setup slice to hold neighbours
	neighbours := make([][]int, width)
	for i := range neighbours {
		neighbours[i] = make([]int, width)
	}

	// if you are not calculating the whole board yourself ( 1 worker)
	if !(start == 0 && end == width) {
		// if you are the first worker
		if start == 0 {
			// get wrap around data from the final row
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
			// otherwise start calculating from one row above your own
			start--
		}

		// if you are the final worker
		if end == width {
			// get neighbours from the first row wrap around
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
			// otherwise calculate neighbours for one row below
			end++
		}
	}

	// for all cells in your calculating range
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

							// wraparound
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

							// add neighbour count
							neighbours[ny][nx]++
						}

					}
				}
			}
		}
	}

	return neighbours
}

func runAliveEvery2(done chan bool) {
	//make the ticker
	ticker := time.NewTicker(2 * time.Second)

	for {
		//see if we need to report the number of alive cells
		select {
		case <-done:
			// stop checking
			return
		case _ = <-ticker.C:
			getCount.setTrue()
		}
	}
}

func paused(c distributorChannels, p Params, world [][]uint8, turns int) {
	//state that the game is paused and continually check for key presses
	c.events <- StateChange{turn.get(), Paused}
	for keyNew := range c.keyPresses {
		switch keyNew {
		case 's':
			// make PGM output for the current turn and world state
			makeOutputTurnWithTurnNum(p, c, turns, world)
		case 'p':
			// unpause and state that execution has started again
			c.events <- StateChange{turn.get(), Executing}
			pause.Done()
			// go back to original key press function
			return
		case 'q':
			// unpause and quit the game
			end.setTrue()
			pause.Done()
			// go back to original key press function
			return
		}
	}
}

func runKeyPressController(c distributorChannels, p Params) {
	// continually check for key presses
	for key := range c.keyPresses {
		switch key {
		case 's':
			// take a snapshot after this turn finishes calculating
			snapshot.setTrue()
		case 'q':
			// quit after this turn finishes calculating
			end.setTrue()
			return
		case 'p':
			// stop the game from quitting as you are performing a key press
			executingKeyPress.Add(1)
			// pause processing
			pause.Add(1)
			// wait for the pause to go into action
			pauseReached.Wait()
			// check for further key presses whilst paused
			paused(c, p, worldGlobal, turn.get())
			executingKeyPress.Done()
		}
	}
}

func executeTurns(p Params, c distributorChannels, world [][]uint8) [][]uint8 {
	// start this wait group for pausing utiity
	pauseReached.Add(1)

	//execute all turn for the GoL
	for turn.get() < p.Turns {
		// call the function to calculate new 255 cells from old 255 cells
		world = calculateNewAliveParallel(p, p.Threads, c, world)
		worldGlobal = world

		// increase the number of turns passed
		turn.inc()

		//pause utility
		pauseReached.Done()
		pause.Wait()
		pauseReached.Add(1)

		// if the game ends prematurely, exit this function with current world
		if end.get() {
			return world
		}

		// if we should return the count, get it a return it
		if getCount.get() {
			getCount.setFalse()
			c.events <- AliveCellsCount{turn.get(), getNumAliveCells(p, world)}
		}

		// if we should output a pgm, do so
		if snapshot.get() {
			snapshot.setFalse()
			makeOutputTurnWithTurnNum(p, c, turn.get(), world)
		}

		// send turn complete for SDL
		c.events <- TurnComplete{turn.get()}
	}
	return world
}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, c distributorChannels) {
	// Create a 2D slice to store the world.
	world := make([][]uint8, p.ImageHeight)
	for i := range world {
		world[i] = make([]uint8, p.ImageWidth)
	}

	// set turns to 0
	turn.reset()

	// add a get input to the command channel
	// give file name
	c.ioCommand <- ioInput
	c.ioFilename <- fmt.Sprintf("%vx%v", p.ImageWidth, p.ImageHeight)

	// for SDL flips
	cells := make([]util.Cell, p.ImageWidth*p.ImageHeight)

	//get the world
	world = <-c.ioInput

	// Make sure that the Io has finished any input before moving on to processing
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	// get flipped cells
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if world[x][y] == 255 {
				cells = append(cells, util.Cell{X: x, Y: y})
			}
		}
	}

	// setup global
	worldGlobal = world

	// flip the cells are start execution
	c.events <- CellsFlipped{turn.get(), cells}
	c.events <- StateChange{turn.get(), Executing}

	// key press controller
	// create stop channel for quitting
	go runKeyPressController(c, p)

	// create a new ticket and a channel to stop it
	// run the ticker
	done := make(chan bool)
	go runAliveEvery2(done)

	// Execute all turns of the Game of Life.
	world = executeTurns(p, c, world)

	//output the slice to a pgm
	if end.get() {
		makeOutputTurnWithTurnNum(p, c, turn.get(), world)
	} else {
		makeOutput(p, c, world)
	}

	//Report the final state using FinalTurnCompleteEvent.
	aliveCells := getAliveCells(p, world)
	c.events <- FinalTurnComplete{turn.get(), aliveCells}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	c.events <- StateChange{turn.get(), Quitting}

	// stop the ticker
	done <- true

	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	executingKeyPress.Wait()
	close(done)
	close(c.events)
}
