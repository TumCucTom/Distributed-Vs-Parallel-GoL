// Example useage:
// go test -run a$ -bench BenchmarkStudentVersion/512x512x1000 -timeout 1000s -cpuprofile cpu.prof
package main

import (
	"fmt"
	"os"
	"testing"
	"uk.ac.bris.cs/gameoflife/gol"
)

const benchLength = 1000

func BenchmarkStudentVersion(b *testing.B) {
	for threads := 1; threads <= 1; threads++ {
		for workers := 1; workers <= 1; workers++ {
			os.Stdout = nil // Disable all program output apart from benchmark results
			p := gol.Params{
				Turns:       benchLength,
				Threads:     threads,
				Workers:     workers,
				ImageWidth:  512,
				ImageHeight: 512,
			}
			name := fmt.Sprintf("%dx%dx%d-%d-%d", p.ImageWidth, p.ImageHeight, p.Turns, p.Workers, p.Threads)
			b.Run(name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					events := make(chan gol.Event)
					go gol.Run(p, events, nil)
					for range events {
					}
				}
			})
		}
	}
}
