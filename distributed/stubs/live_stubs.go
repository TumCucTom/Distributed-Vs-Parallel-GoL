package stubs

import "uk.ac.bris.cs/gameoflife/util"

var GiveInfo = "LiveView.TakeInfo"

type LiveRequest struct {
	Flipped []util.Cell
	Turn    int
}

type LiveResponse struct {
}
