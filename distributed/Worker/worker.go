//package main
//
//import (
//	"flag"
//	"log"
//	"net"
//	"net/rpc"
//	"uk.ac.bris.cs/gameoflife/stubs"
//)
//
//type Node struct{}
//
//var quitting = make(chan bool, 1)
//
//func (n *Node) GetSegment(req stubs.WorkerRequest, res *stubs.WorkerResponse) error {
//	res.Segment = calculateNextWorld(req.WholeWorld, req.Start, req.End, req.Size)
//	return nil
//}
//
//func (n *Node) Quit(_ stubs.WorkerRequest, _ *stubs.WorkerResponse) error {
//	quitting <- true
//	return nil
//}
//
//func calculateNextWorld(world [][]uint8, start, end, width int) [][]uint8 {
//	newWorld := make([][]uint8, end-start)
//	for i := 0; i < end-start; i++ {
//		newWorld[i] = make([]uint8, width)
//	}
//	for y := start; y < end; y++ {
//		for x := 0; x < width; x++ {
//			neighbors := calculateNeighbor(x, y, world, width)
//			if neighbors < 2 || neighbors > 3 {
//				newWorld[y-start][x] = 0
//			} else if neighbors == 3 {
//				newWorld[y-start][x] = 255
//			} else if neighbors == 2 && world[y][x] == 255 {
//				newWorld[y-start][x] = 255
//			} else {
//				newWorld[y-start][x] = 0
//			}
//		}
//	}
//	return newWorld
//}
//
//func calculateNeighbor(x, y int, world [][]uint8, width int) int {
//	aliveNeighbor := 0
//	for i := -1; i <= 1; i++ {
//		for j := -1; j <= 1; j++ {
//			ny, nx := y+i, x+j
//			if nx < 0 {
//				nx = width - 1
//			} else if nx == width {
//				nx = 0
//			} else {
//				nx = nx % width
//			}
//
//			if ny < 0 {
//				ny = width - 1
//			} else if ny == width {
//				ny = 0
//			} else {
//				ny = ny % width
//			}
//
//			if world[ny][nx] == 255 {
//				if !(i == 0 && j == 0) {
//					aliveNeighbor++
//				}
//			}
//		}
//	}
//	return aliveNeighbor
//}
//
//func main() {
//	// NOT IN USE //
//	serverPort := flag.String("port", "8040", "Port to Listen")
//	flag.Parse()
//
//	rpc.Register(&Node{})
//	listener, err := net.Listen("tcp", "0.0.0.0:"+*serverPort)
//	if err != nil {
//		log.Fatal("Listener error:", err)
//	}
//	log.Println("Server listening on port", *serverPort)
//	defer listener.Close()
//	go rpc.Accept(listener)
//
//	_ = <-quitting
//}
