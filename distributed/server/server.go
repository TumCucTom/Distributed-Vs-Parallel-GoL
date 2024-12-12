package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"uk.ac.bris.cs/gameoflife/stubs"
	"uk.ac.bris.cs/gameoflife/util"
)

var distWorkerNum, threadNum int

var quitting = make(chan bool, 1)

type RestartInfo struct {
	restart bool
	turns   int
	world   [][]uint8
}

type BoolContainer struct {
	mu     sync.Mutex
	status bool
}

type IntContainer2 struct {
	mu    sync.Mutex
	value int
	turn  int
}

type IntContainer struct {
	mu   sync.Mutex
	turn int
}

type WorldContainer struct {
	mu    sync.Mutex
	world [][]uint8
	turn  int
}

type LargeWorldContainer struct {
	mu     sync.Mutex
	worlds [][][]uint8
}

func (c *LargeWorldContainer) setAtIndex(i int, w [][]uint8) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.worlds[i] = w
}

func (c *LargeWorldContainer) getAtIndex(i int) [][]uint8 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.worlds[i]
}

func (c *LargeWorldContainer) setup(u [][][]uint8) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.worlds = u
}

func (c *BoolContainer) get() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.status
}

func (c *BoolContainer) setTrue() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.status = true
}

func (c *BoolContainer) setFalse() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.status = false
}

func (c *IntContainer2) getTurn() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.turn
}

func (c *IntContainer) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.turn
}

func (c *IntContainer) set(val int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.turn = val
}

func (c *IntContainer2) getCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.value
}

func (w *WorldContainer) getWorld() [][]uint8 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.world
}
func (w *WorldContainer) getTurn() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.turn
}

func (w *WorldContainer) set(new [][]uint8, turn int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.world = new
	w.turn = turn
}

func (w *WorldContainer) setWorld(new [][]uint8) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.world = new
}

func (w *WorldContainer) setTurn(new int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.turn = new
}

func (c *IntContainer2) set(turnsCompleted, count int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.value = count
	c.turn = turnsCompleted
}

var pausedTurn IntContainer
var getCount, paused, snapShot, quit, shut, clientQuit BoolContainer
var aliveCount IntContainer2
var waitingSnapShot, pause, waitingForCount, waitingShut, makingSnapshot sync.WaitGroup
var world, snapshotInfo WorldContainer
var restartInformation RestartInfo

func getAliveCells(height, width int, world [][]uint8) []util.Cell {
	// iterate through the world and store alive cells
	aliveCells := make([]util.Cell, 0)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			if world[x][y] == 255 {
				aliveCells = append(aliveCells, util.Cell{X: x, Y: y})
			}
		}
	}
	return aliveCells
}

func getAliveCellsFor(world [][]uint8, height, width int) int {
	// iterate through the world anc count the number of alive cells
	count := 0
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			if world[x][y] == 255 {
				count++
			}
		}
	}
	return count
}

type Server struct{}

func (s *Server) GetAliveCells(_ stubs.RequestAlive, res *stubs.ResponseAlive) error {
	// allow the controller to request alive cells (for ticker)

	// ask for data, wait for data
	getCount.setTrue()
	waitingForCount.Add(1)
	waitingForCount.Wait()

	// return the given data
	res.NumAlive = aliveCount.getCount()
	res.Turn = aliveCount.getTurn()
	return nil
}

func (s *Server) GetSnapshot(_ stubs.RequestAlive, res *stubs.ResponseSnapshot) error {
	// we want to get back the state of the board

	// ask and wait for data
	snapShot.setTrue()
	waitingSnapShot.Add(1)
	waitingSnapShot.Wait()

	//return the given data
	res.Turns = snapshotInfo.getTurn()
	res.NewWorld = snapshotInfo.getWorld()
	return nil
}

func (s *Server) GetSnapshotPaused(_ stubs.RequestAlive, res *stubs.ResponseSnapshot) error {
	// we want to get back the state of the board

	// state we are processing a PGM
	makingSnapshot.Add(1)

	// return the data
	res.NewWorld = world.getWorld()
	res.Turns = world.turn

	// allow processing to continue
	makingSnapshot.Done()
	return nil
}

func (s *Server) PauseProcessing(_ stubs.EmptyReq, res *stubs.ResponseTurn) error {
	// pause the processing
	pause.Add(1)
	paused.setTrue()

	// return the turn paused on
	res.Turn = pausedTurn.get()
	return nil
}
func (s *Server) Quit(_ stubs.EmptyReq, _ *stubs.EmptyRes) error {
	// quit distributed system gracefully once next turn is complete
	quit.setTrue()
	return nil
}

func (s *Server) ClientQuit(_ stubs.EmptyReq, _ *stubs.EmptyRes) error {
	// client is stating that it will disconnect
	clientQuit.setTrue()
	return nil
}

func (s *Server) ClientQuitPause(_ stubs.EmptyReq, _ *stubs.EmptyRes) error {
	// client is stating that it will disconnect whilst paused
	clientQuit.setTrue()
	pause.Done()
	return nil
}

func (s *Server) UnpauseProcessing(_ stubs.EmptyReq, _ *stubs.EmptyRes) error {
	// allow processing to resume
	paused.setFalse()
	pause.Done()
	return nil
}

func makeNewWorld(height, width int) [][]uint8 {
	// make a new empty 2d slice
	newWorld := make([][]uint8, height)
	for i := range newWorld {
		newWorld[i] = make([]uint8, width)
	}
	return newWorld
}

func runWorker(client *rpc.Client, size, start, end, threads int, currentWorld [][]uint8, splitSegment chan [][]uint8) {
	// give the distributed node the relevant info and process the GoL turn for a given range
	res := new(stubs.ParallelWorkerResponse)
	req := stubs.ParallelWorkerRequest{
		WholeWorld: currentWorld,
		Start:      start,
		End:        end,
		Size:       size,
		Threads:    threads,
	}
	// call the node
	if err := client.Call(stubs.CalculateWorldSegmentParallel, req, res); err != nil {
		fmt.Println(err)
	}
	// accept the world as result
	splitSegment <- res.Segment

	// if not calcuating using xor in broker uncomment this
	//flipped <- res.Flipped
}

func setupWorkers(workers []*rpc.Client, threads, size, workerNum int, currentWorld [][]uint8, splitSegments []chan [][]uint8) {
	// get the number of row that the first n-1 workers should work with
	numRows := size / workerNum

	i := 0
	// run all but the last worker for specified rows
	for i < workerNum-1 {
		go runWorker(workers[i], size, i*numRows, numRows*(i+1), threads, currentWorld, splitSegments[i])
		i++
	}

	// final worker does the remaining rows
	go runWorker(workers[i], size, i*numRows, size, threads, currentWorld, splitSegments[i])
}

func closeWorkers(workers []*rpc.Client) {
	req := new(stubs.Empty)
	res := new(stubs.Empty)
	// for all workers call their kill method which cleanly shuts them down
	for _, worker := range workers {
		if err := worker.Call(stubs.End, req, res); err != nil {
			fmt.Println(err)
		}
		worker.Close()
	}
}

func giveFlipped(sdlClient *rpc.Client, flipped []util.Cell, turn int) {
	// give the flipped cells to the SDL controller
	req := stubs.LiveRequest{Flipped: flipped, Turn: turn}
	res := stubs.EmptyRes{}
	// call SDL controller
	if err := sdlClient.Call(stubs.GiveInfo, req, &res); err != nil {
		fmt.Println(err)
	}
}

func dialWorkers(workerNum int) []*rpc.Client {
	// store servers and ports for the distributed nodes
	serverAddress := "127.0.0.1"
	wholeWorkerPorts := [8]string{":9010", ":9000", ":8090", ":8080", ":8070", ":8060", ":8050", ":8040"}
	workerPorts := wholeWorkerPorts[8-workerNum : 8]

	// uncomment and change for AWS
	//workerPrivateAddress := [8]string{"172.31.24.115:8040", "172.31.18.64:8050", "172.31.31.193:8060", "172.31.21.74:8070", "172.31.17.226:8080", "172.31.26.116:8090", "172.31.26.101:9000"}

	// make a list of workers
	workers := make([]*rpc.Client, workerNum)

	// dial the first worker
	worker, err := rpc.Dial("tcp", fmt.Sprintf("%v%v", serverAddress, workerPorts[workerNum-1]))

	for i := workerNum - 2; i >= 0; i-- {
		// dial remaining workers one by one
		worker, err := rpc.Dial("tcp", fmt.Sprintf("%v%v", serverAddress, workerPorts[i]))

		// get workers to connect to their neighbour for halo
		res := new(stubs.Empty)
		req := stubs.AddressReq{Address: fmt.Sprintf("%v%v", serverAddress, workerPorts[i+1])}
		worker.Call(stubs.Connect, req, res)

		// add worker to the slice
		workers[i] = worker

		if err != nil {
			fmt.Println(err)
		}
	}

	// connect the last first worker to the last
	res := new(stubs.Empty)
	req := stubs.AddressReq{Address: fmt.Sprintf("%v%v", serverAddress, workerPorts[0])}
	worker.Call(stubs.Connect, req, res)

	// add this worker
	workers[workerNum-1] = worker

	if err != nil {
		fmt.Println(err)
	}

	return workers
}

func calculateNextWorld(workers []*rpc.Client, currentWorld [][]uint8, size, workerNum, threads int) [][]uint8 {
	var newWorld [][]uint8

	// if not calcuating using xor in broker uncomment this
	//var flipped []util.Cell

	// create smaller segments for each worker to calculate
	splitSegments := make([]chan [][]uint8, workerNum)
	// if not calcuating using xor in broker uncomment this
	//splitFlipped := make([]chan []util.Cell, workerNum)
	for i := range splitSegments {
		splitSegments[i] = make(chan [][]uint8)
		// if not calcuating using xor in broker uncomment this
		//splitFlipped[i] = make(chan []util.Cell)
	}

	// start the workers
	setupWorkers(workers, threads, size, workerNum, currentWorld, splitSegments)

	// append each segment
	for i := 0; i < workerNum; i++ {
		newWorld = append(newWorld, <-splitSegments[i]...)
		//flipped = append(flipped, <-splitFlipped[i]...)
	}

	return newWorld
}

func dialSdl() *rpc.Client {
	// dial the SDL client
	sdlAddress := "127.0.0.1:9020"
	worker, err := rpc.Dial("tcp", sdlAddress)
	if err != nil {
		fmt.Println(err)
	}
	return worker
}

func calculateFlipped(new [][]uint8, old [][]uint8) []util.Cell {
	// perform cell wise xor for flipped cells and return
	var cells []util.Cell

	for i, item := range new {
		for j := range item {
			if new[i][j] != old[i][j] {
				cells = append(cells, util.Cell{j, i})
			}
		}
	}
	return cells
}

func (s *Server) ProcessTurns(req stubs.Request, res *stubs.Response) error {
	// make an empty array for flipped cells
	var flipped []util.Cell

	// set the current world from the controller's pull from IO
	currentWorld := req.OldWorld
	//make and empty world
	nextWorld := makeNewWorld(req.ImageHeight, req.ImageWidth)
	turn := 0

	// if we are restarting, then load the information
	if req.Restart {
		if restartInformation.restart {
			currentWorld = restartInformation.world
			turn = restartInformation.turns
		} else {
			return errors.New("nothing to restart with")
		}
	}

	// dial all workers and the controller
	workers := dialWorkers(distWorkerNum)
	sdlClient := dialSdl()

	//execute all turns of GoL
	for turnNum := 0; turnNum < req.Turns; turnNum++ {

		// calculate new world and the flipped cells from new world
		nextWorld = calculateNextWorld(workers, currentWorld, req.ImageHeight, distWorkerNum, threadNum)
		flipped = calculateFlipped(nextWorld, currentWorld)

		// update the params
		turn = turnNum + 1
		currentWorld = nextWorld
		world.set(currentWorld, turn)

		// give the flipped cells to SDL
		giveFlipped(sdlClient, flipped, turn)

		// wait if we are currently taking a snapshot
		makingSnapshot.Wait()

		// if pause
		if paused.get() {
			// wait to be unpaused and give the turn from controller res
			pausedTurn.set(turn)
			pause.Wait()
		}

		// if quitting
		if quit.get() {
			// exit GoL execution
			break
		}

		// if making a PGM
		if snapShot.get() {
			// give the information and state that we are done giving the information
			snapShot.setFalse()
			snapshotInfo.set(currentWorld, turn)
			waitingSnapShot.Done()
		}
		// if we need to get the count for the ticker
		if getCount.get() {
			// give the information and state that we are done giving the information
			getCount.setFalse()
			aliveCount.set(turn, getAliveCellsFor(currentWorld, req.ImageHeight, req.ImageWidth))
			waitingForCount.Done()
		}
		// if the client choses to disconnect
		if clientQuit.get() {
			// store restart information for if client wishes to reconnect
			restartInformation = RestartInfo{restart: true, turns: turn, world: currentWorld}
			clientQuit.setFalse()
			break
		}
	}

	// return information back to the controller
	res.Turns = turn
	res.NewWorld = currentWorld
	res.AliveCellLocation = getAliveCells(req.ImageHeight, req.ImageWidth, currentWorld)

	// if we are killing the distributed system
	if quit.get() {
		// kill the workers
		closeWorkers(workers)
		// exit from main
		quitting <- true
	}
	return nil
}

func main() {
	// allow a controller to connect to us
	serverPort := flag.String("port", "8030", "Port to Listen")
	workers := flag.Int("workers", 1, "How many workers to use")
	threads := flag.Int("threads", 1, "How many threads on each worker to use")
	flag.Parse()

	// parse flags for system setup
	distWorkerNum = *workers
	threadNum = *threads

	// dial the controller for SDL updates
	rpc.Register(&Server{})
	listener, err := net.Listen("tcp", "127.0.0.1:"+*serverPort)
	if err != nil {
		log.Fatal("Listener error:", err)
	}
	log.Println("Server listening on port", *serverPort)
	defer listener.Close()
	go rpc.Accept(listener)

	_ = <-quitting
}
