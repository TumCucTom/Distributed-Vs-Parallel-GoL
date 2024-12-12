package gol

import (
	"os"
	"strconv"
	"strings"
	"uk.ac.bris.cs/gameoflife/util"
)

type ioChannels struct {
	command <-chan ioCommand
	idle    chan<- bool

	filename <-chan string
	output   <-chan [][]uint8
	input    chan<- [][]uint8
}

// ioState is the internal ioState of the io goroutine.
type ioState struct {
	params   Params
	channels ioChannels
}

// ioCommand allows requesting behaviour from the io (pgm) goroutine.
type ioCommand uint8

// This is a way of creating enums in Go.
// It will evaluate to:
//
//	ioOutput 	= 0
//	ioInput 	= 1
//	ioCheckIdle = 2
const (
	ioOutput ioCommand = iota
	ioInput
	ioCheckIdle
)

// writePgmImage receives an array of bytes and writes it to a pgm file.
func (io *ioState) writePgmImage() {
	_ = os.Mkdir("out", os.ModePerm)

	// Request a filename from the distributor.
	filename := <-io.channels.filename

	file, ioError := os.Create("out/" + filename + ".pgm")
	util.Check(ioError)
	defer file.Close()

	_, _ = file.WriteString("P5\n")
	//_, _ = file.WriteString("# PGM file writer by pnmmodules (https://github.com/owainkenwayucl/pnmmodules).\n")
	_, _ = file.WriteString(strconv.Itoa(io.params.ImageWidth))
	_, _ = file.WriteString(" ")
	_, _ = file.WriteString(strconv.Itoa(io.params.ImageHeight))
	_, _ = file.WriteString("\n")
	_, _ = file.WriteString(strconv.Itoa(255))
	_, _ = file.WriteString("\n")

	world := <-io.channels.output

	for y := 0; y < io.params.ImageHeight; y++ {
		for x := 0; x < io.params.ImageWidth; x++ {
			_, ioError = file.Write([]byte{world[x][y]})
			util.Check(ioError)
		}
	}

	ioError = file.Sync()
	util.Check(ioError)

	//fmt.Println("File", filename, "output done!")
}

// readPgmImage opens a pgm file and sends its data as an array of bytes.
func (io *ioState) readPgmImage() {

	// Request a filename from the distributor.
	filename := <-io.channels.filename

	data, ioError := os.ReadFile("images/" + filename + ".pgm")
	util.Check(ioError)

	fields := strings.Fields(string(data))

	if fields[0] != "P5" {
		panic("Not a pgm file")
	}

	width, _ := strconv.Atoi(fields[1])
	if width != io.params.ImageWidth {
		panic("Incorrect width")
	}

	height, _ := strconv.Atoi(fields[2])
	if height != io.params.ImageHeight {
		panic("Incorrect height")
	}

	maxval, _ := strconv.Atoi(fields[3])
	if maxval != 255 {
		panic("Incorrect maxval/bit depth")
	}

	image := []byte(fields[4])

	world := make([][]uint8, io.params.ImageHeight)
	for i := range world {
		world[i] = make([]uint8, io.params.ImageWidth)
	}

	count := 0
	for _, b := range image {
		world[count%io.params.ImageWidth][count/io.params.ImageHeight] = b
		count++
	}

	io.channels.input <- world

	//fmt.Println("File", filename, "input done!")
}

// startIo should be the entrypoint of the io goroutine.
func startIo(p Params, c ioChannels) {
	io := ioState{
		params:   p,
		channels: c,
	}

	for command := range io.channels.command {
		// Block and wait for requests from the distributor
		switch command {
		case ioInput:
			io.readPgmImage()
		case ioOutput:
			io.writePgmImage()
		case ioCheckIdle:
			io.channels.idle <- true
		}
	}
}
