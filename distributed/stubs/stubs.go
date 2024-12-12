package stubs

import "uk.ac.bris.cs/gameoflife/util"

var Turns = "Server.ProcessTurns"
var Alive = "Server.GetAliveCells"
var QuitServer = "Server.Quit"
var Snapshot = "Server.GetSnapshot"
var PausedSnapshot = "Server.GetSnapshotPaused"
var Pause = "Server.PauseProcessing"
var Unpause = "Server.UnpauseProcessing"
var QuitClient = "Server.ClientQuit"
var QuitClientPaused = "Server.ClientQuitPause"

type AliveCellsRequest struct {
}

type AliveCellsResponse struct {
	AliveCount int
}

type ResponseTurn struct {
	Turn int
}

type Response struct {
	NewWorld          [][]uint8
	Turns             int
	AliveCell         int
	AliveCellLocation []util.Cell
}

type Request struct {
	OldWorld    [][]uint8
	Turns       int
	Threads     int
	Workers     int
	ImageWidth  int
	ImageHeight int
	Start       int
	End         int
	Restart     bool
}

type Empty struct {
}

type RequestAlive struct {
}
type ResponseAlive struct {
	Turn     int
	NumAlive int
}

type RequestKeyPress struct {
	KeyPress rune
}
type ResponseKey struct {
	World        [][]uint8
	Turn         int
	Acknowledged bool
}

type ResponseSnapshot struct {
	NewWorld [][]uint8
	Turns    int
}

type EmptyRes struct {
}

type EmptyReq struct {
}
