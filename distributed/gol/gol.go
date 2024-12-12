package gol

// Params provides the details of how to run the Game of Life and which image to load.
type Params struct {
	Turns       int
	Threads     int
	Workers     int
	ImageWidth  int
	ImageHeight int
}

// Run starts the processing of Game of Life. It should initialise channels and goroutines.
func Run(p Params, events chan<- Event, keyPresses <-chan rune) {
	// this must be commented out for testing and the line at the bottom should be uncommented
	//restart := flag.Bool("restart", false, "Did you disconnect and would like to continue")
	//flag.Parse()

	//	TODO: Put the missing channels in here.

	ioCommand := make(chan ioCommand)
	ioIdle := make(chan bool)
	ioFilename := make(chan string)
	ioOutput := make(chan uint8, p.ImageWidth*p.ImageHeight)
	ioInput := make(chan uint8, p.ImageWidth*p.ImageHeight)

	ioChannels := ioChannels{
		command:  ioCommand,
		idle:     ioIdle,
		filename: ioFilename,
		output:   ioOutput,
		input:    ioInput,
	}
	go startIo(p, ioChannels)

	distributorChannels := distributorChannels{
		events:     events,
		ioCommand:  ioCommand,
		ioIdle:     ioIdle,
		ioFilename: ioFilename,
		ioOutput:   ioOutput,
		ioInput:    ioInput,
		keyPresses: keyPresses,
	}

	distributor(p, distributorChannels, false)
	//distributor(p, distributorChannels, *restart)

}
