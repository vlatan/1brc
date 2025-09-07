package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

type Station struct {
	Min, Max, Count, Sum int64
}

type Stations map[string]Station

var filePath = flag.String("f", "", "path to the input file")

func main() {

	flag.Parse()

	start := time.Now()

	stations, err := mapStations(*filePath)
	if err != nil {
		log.Fatalf("Error mapping the stations: %v", err)
	}

	fmt.Println(stations)
	fmt.Println("Time took:", time.Since(start))
}

// mapStations puts stations from file into a map with all the necessary stats
func mapStations(filePath string) (Stations, error) {

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("unable to open the file: %w", err)
	}
	defer file.Close()

	chunkSize := 64 * 1024 * 1024 // 64MiB
	numWorkers := runtime.NumCPU() - 1
	results := make(chan Stations)
	chunks := make(chan []byte, 10)
	var wg sync.WaitGroup

	// Spawn workers in the background
	for range numWorkers {
		wg.Go(func() { worker(chunks, results) })
	}

	// Spawn a background job that reads the file
	// in chunks and sends them to the chunks channel
	go func() {

		defer func() {
			wg.Wait()
			close(results)
		}()

		defer close(chunks)

		buf := make([]byte, chunkSize)
		var leftover []byte

		for {
			bytesRead, err := file.Read(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				log.Fatalf("Unable to read the file: %v", err)
			}

			// file.Read(buf) might read less than 64MiB (especially near EOF)
			// The unread portion of buf contains garbage/old data
			// So we need the data that was ACTUALLY read
			data := buf[:bytesRead]

			// Determine where is the last '\n' in the data
			lastNLIndex := bytes.LastIndex(data, []byte{'\n'})

			// Length of the previous leftover + what we need of the current data
			chunk := make([]byte, len(leftover)+lastNLIndex+1)
			// Copy the previous leftover to the begining of the chunk
			copy(chunk, leftover)
			// Copy what we need of the buffer to the rest of the chunk
			copy(chunk[len(leftover):], data[:lastNLIndex+1])

			// Make new leftover
			currentLeftover := data[lastNLIndex+1:]
			leftover = make([]byte, len(currentLeftover))
			copy(leftover, currentLeftover)

			// Send chunk to channel
			chunks <- chunk
		}
	}()

	// Collect the results
	stations := make(Stations)
	for result := range results {
		for name, stats := range result {
			st, ok := stations[name]
			if !ok {
				st.Min = stats.Min
				st.Max = stats.Max
			} else {
				st.Max = max(st.Max, stats.Max)
				st.Min = min(st.Min, stats.Min)
			}

			st.Count += stats.Count
			st.Sum += stats.Sum
			stations[name] = st
		}
	}

	return stations, nil
}

// worker consumes a chunk from the chunks channel,
// produces a result map and sends it to the results channel
func worker(chunks chan []byte, results chan Stations) {

	for chunk := range chunks {

		s := make(Stations)
		var cursor int
		var name string

		for i, char := range chunk {

			switch char {
			case ';':
				name = string(chunk[cursor:i])
				cursor = i + 1
			case '\n':
				temp := parseTemp(chunk[cursor:i])
				cursor = i + 1

				st, ok := s[name]
				if !ok {
					st.Min = temp
					st.Max = temp
				} else {
					st.Max = max(st.Max, temp)
					st.Min = min(st.Min, temp)
				}

				st.Count++
				st.Sum += temp
				s[name] = st
			}
		}

		results <- s
	}
}

// sortNames returns a slice of sorted station names
func (s Stations) sortNames() []string {
	names := make([]string, len(s))
	var i int

	for name := range s {
		names[i] = name
		i++
	}

	sort.Strings(names)
	return names
}

// parseTemp converts string to int64
func parseTemp(temp []byte) (result int64) {
	var neg bool
	if temp[0] == '-' {
		neg = true
		temp = temp[1:]
	}

	// Look up the ASCII table codes for digits and this clever math pans out
	switch len(temp) {
	case 3:
		// Example "2.5"
		// 50*10 + 53 - 48*11 = 25
		result = int64(temp[0])*10 + int64(temp[2]) - int64('0')*11
	case 4:
		// Example "12.5"
		// 49*100 + 50*10 + 53 - 48*111 = 125
		result = int64(temp[0])*100 + int64(temp[1])*10 + int64(temp[3]) - (int64('0') * 111)
	default:
		log.Fatalf("Unable to parse temperature to int64: %s", temp)
	}

	if neg {
		return -result
	}

	return
}

// String creates a string respresentation from stations map
// which makes the stations implement the Stringer interface
func (s Stations) String() string {

	var sb strings.Builder
	sb.WriteString("{")

	for i, name := range s.sortNames() {
		if i > 0 {
			sb.WriteString(", ")
		}

		stats := s[name]
		minTemp := float64(stats.Min) / 10.0
		maxTemp := float64(stats.Max) / 10.0
		avgTemp := float64(stats.Sum) / float64(stats.Count) / 10.0
		statsStr := fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, minTemp, avgTemp, maxTemp)
		sb.WriteString(statsStr)
	}

	sb.WriteString("}")
	return sb.String()
}
