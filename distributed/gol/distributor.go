package gol

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
	"uk.ac.bris.cs/gameoflife/stubs"
	"uk.ac.bris.cs/gameoflife/util"
)

var quit bool

var sdlUpdated chan bool

var waitSDL sync.WaitGroup

type LiveView struct{}

var sdlCells cellsFlipped

type cellsFlipped struct {
	mu    sync.Mutex
	cells []util.Cell
	turn  int
}

func (c *cellsFlipped) read() ([]util.Cell, int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cells, c.turn
}

func (c *cellsFlipped) write(new []util.Cell, turns int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cells = new
	c.turn = turns
}

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
	keyPresses <-chan rune
}

func (s *LiveView) TakeInfo(req stubs.LiveRequest, _ *stubs.LiveResponse) error {
	// allow the broker to call us to update SDL information
	waitSDL.Add(1)
	// write the new cells
	sdlCells.write(req.Flipped, req.Turn)
	// state that we are ready to update SDL
	sdlUpdated <- true
	waitSDL.Wait()
	return nil
}

// Create and initialize a new 2D grid with given dimensions
func initializeWorld(height, width int) [][]uint8 {
	// make an empty 2d slice of the size of the world
	world := make([][]uint8, height)
	for i := range world {
		world[i] = make([]uint8, width)
	}
	return world
}

func loadInitialState(p Params, c distributorChannels) [][]uint8 {
	// make a world
	world := initializeWorld(p.ImageHeight, p.ImageWidth)
	// get relevant data from IO
	filename := fmt.Sprintf("%vx%v", p.ImageWidth, p.ImageHeight)
	c.ioCommand <- ioInput
	c.ioFilename <- filename

	// make the world byte by byte

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			value := <-c.ioInput
			world[x][y] = value

			if value == 255 {
				// flip cells to start SDL live view
				c.events <- CellFlipped{
					CompletedTurns: 0,
					Cell:           util.Cell{X: x, Y: y},
				}
			}
		}
	}
	// state 0th turn complete
	c.events <- TurnComplete{0}

	// check we are done with input and return
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
	return world
}

func saveGameState(p Params, c distributorChannels, turns int, world [][]uint8) {
	// give information to IO
	c.ioCommand <- ioOutput
	filename := fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, turns)
	c.ioFilename <- filename

	// give each byte of state to IO
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.ioOutput <- world[x][y]
		}
	}

	// check processing is finished and then state we have outputted
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
	c.events <- ImageOutputComplete{turns, filename}
}

// Send an RPC call to the server and retrieve the updated game state
func executeTurn(client *rpc.Client, req stubs.Request, res *stubs.Response) {
	// start GoL execution on broker via rpc call
	if err := client.Call(stubs.Turns, req, &res); err != nil {
		fmt.Println(err)
	}
}

func getCount(client *rpc.Client, c distributorChannels) {
	// make an rpc call to the broker to get the current alice count
	res := new(stubs.ResponseAlive)
	if err := client.Call(stubs.Alive, stubs.EmptyReq{}, &res); err != nil {
		fmt.Println(err)
	}
	// display the count response
	c.events <- AliveCellsCount{res.Turn, res.NumAlive}
}

func quitServer(client *rpc.Client) {
	// force a clean kill of all distributed components via rpc call to broker
	res := stubs.EmptyRes{}
	if err := client.Call(stubs.QuitServer, stubs.EmptyReq{}, &res); err != nil {
		fmt.Println(err)
	}
}

func quitClient(client *rpc.Client) {
	// state you will quit the client
	res := stubs.EmptyRes{}
	if err := client.Call(stubs.QuitClient, stubs.EmptyReq{}, &res); err != nil {
		fmt.Println(err)
	}
}

func quitClientPaused(client *rpc.Client) {
	// state you will quit the client whilst execution is paused
	res := stubs.EmptyRes{}
	if err := client.Call(stubs.QuitClientPaused, stubs.EmptyReq{}, &res); err != nil {
		fmt.Println(err)
	}
}

func pauseClient(client *rpc.Client) int {
	// make a rpc call to stop client processing
	res := new(stubs.ResponseTurn)
	if err := client.Call(stubs.Pause, stubs.EmptyReq{}, &res); err != nil {
		fmt.Println(err)
	}
	return res.Turn
}

func unpauseClient(client *rpc.Client) {
	// make a rpc call to resume client processing
	if err := client.Call(stubs.Unpause, stubs.EmptyReq{}, &stubs.EmptyRes{}); err != nil {
		fmt.Println(err)
	}
}

func snapshot(client *rpc.Client, p Params, c distributorChannels) {
	// make an rpc call to the client
	res := new(stubs.ResponseSnapshot)
	if err := client.Call(stubs.Snapshot, stubs.EmptyReq{}, &res); err != nil {
		fmt.Println(err)
	}
	// create a PGM output using the returned turn and state
	saveGameState(p, c, res.Turns, res.NewWorld)
}

func pausedSnapshot(client *rpc.Client, p Params, c distributorChannels) {
	// make an rpc call to the client
	res := new(stubs.ResponseSnapshot)
	if err := client.Call(stubs.PausedSnapshot, stubs.EmptyReq{}, &res); err != nil {
		fmt.Println(err)
	}
	// create a PGM output using the returned turn and state
	saveGameState(p, c, res.Turns, res.NewWorld)
}

func runTicker(done chan bool, client *rpc.Client, c distributorChannels) {
	// start a ticker
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
			// if done, stop running this function
		case _ = <-ticker.C:
			getCount(client, c)
			// give the count upon each tick
		}
	}
}

func paused(client *rpc.Client, c distributorChannels, p Params) {
	// state that you are paused
	turn := pauseClient(client)
	c.events <- StateChange{turn, Paused}

	for keyNew := range c.keyPresses {
		switch keyNew {
		case 's':
			// get PGM output
			pausedSnapshot(client, p, c)
		case 'p':
			// resume broker execution and state you are resuming
			unpauseClient(client)
			c.events <- StateChange{turn, Executing}
			return
		case 'q':
			// quit this controller
			quitClientPaused(client)
			quit = true
			return
		}
	}
}

func runKeyPressController(client *rpc.Client, c distributorChannels, p Params) {
	// continually check for keypresses
	for key := range c.keyPresses {
		switch key {
		case 'k':
			// kill all components gracefully
			quitServer(client)
			return
		case 's':
			// return PGM output
			snapshot(client, p, c)
		case 'q':
			// quit this controller
			quitClient(client)
			quit = true
			return
		case 'p':
			// pause processing on broker
			paused(client, c, p)
		}
	}
}

func copyOf(world [][]uint8, p Params) [][]uint8 {
	// make a copy of a slice
	worldNew := initializeWorld(p.ImageHeight, p.ImageWidth)
	copy(worldNew, world)
	return worldNew
}

func updateSDL(done chan bool, c distributorChannels) {
	for {
		select {
		case <-done:
			// stop running if done
			return
		case <-sdlUpdated:
			// upon receiving flipped cells, run relevant events for SDL
			cells, turn := sdlCells.read()
			c.events <- CellsFlipped{turn, cells}
			c.events <- TurnComplete{turn + 1}
			waitSDL.Done()
		}
	}
}

// Manage client-server interaction and distribute work across routines
func distributor(p Params, c distributorChannels, restart bool) {
	// connect to the broker
	serverAddress := "127.0.0.1:8030"
	client, err := rpc.Dial("tcp", serverAddress)
	if err != nil {
		log.Fatal("dialing", err)
	}
	defer client.Close()

	// let the broker connect to us
	rpc.Register(&LiveView{})
	listener, err := net.Listen("tcp", "127.0.0.1:9020")
	if err != nil {
		log.Fatal("Listener error:", err)
	}
	log.Println("Server listening on port 9020")
	defer listener.Close()
	go rpc.Accept(listener)

	// get the initial world state and commence execution
	initialWorld := loadInitialState(p, c)
	c.events <- StateChange{0, Executing}

	// get req and res for execution
	req := stubs.Request{
		OldWorld:    initialWorld,
		Turns:       p.Turns,
		Threads:     p.Threads,
		Workers:     p.Workers,
		ImageWidth:  p.ImageWidth,
		ImageHeight: p.ImageHeight,
		Restart:     restart,
	}
	res := new(stubs.Response)

	// to end these two go routines
	done := make(chan bool)
	sdlUpdated = make(chan bool, 1)
	defer close(done)

	// start utility goroutines
	go runTicker(done, client, c)
	go runKeyPressController(client, c, p)
	go updateSDL(done, c)

	// start execution on broker
	executeTurn(client, req, res)

	// get the PGM output and state that processing is finished
	saveGameState(p, c, res.Turns, copyOf(res.NewWorld, p))
	c.events <- FinalTurnComplete{
		CompletedTurns: res.Turns,
		Alive:          res.AliveCellLocation,
	}

	// check output is done
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	// quit safely
	c.events <- StateChange{res.Turns, Quitting}
	done <- true
	done <- true
	close(c.events)
}
