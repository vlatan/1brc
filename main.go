package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Station struct {
	Min, Max, Count, Sum float64
}

type Stations map[string]Station

func main() {

	start := time.Now()

	stations, err := mapStations("measurements.txt")
	if err != nil {
		log.Fatalf("Error mapping the stations: %v", err)
	}

	fmt.Println(stations)
	fmt.Println("Time took:", time.Since(start))
}

// mapStations puts stations from file into a map with all the necessary stats
func mapStations(filePath string) (Stations, error) {

	// Lines in a chunk.
	// The more lines, the more memory consumed and
	// a biger chance for the OS to kill this program.
	chunkSize := 5_000_000

	numWorkers := runtime.NumCPU() - 1
	results := make(chan Stations)
	chunks := make(chan []string)
	var wg sync.WaitGroup

	for range numWorkers {
		wg.Go(func() { worker(chunks, results) })
	}

	go func() {
		defer close(chunks)
		file, _ := os.Open(filePath)
		defer file.Close()

		chunk := make([]string, 0, chunkSize)
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			chunk = append(chunk, scanner.Text())
			if len(chunk) == chunkSize {
				chunks <- chunk
				chunk = make([]string, 0, chunkSize)
			}
		}

		if len(chunk) > 0 {
			chunks <- chunk
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	stations := make(Stations)
	for result := range results {
		for name, stats := range result {
			st, ok := stations[name]
			if !ok {
				st.Min = stats.Min
				st.Max = stats.Max
			}

			st.Max = max(st.Max, stats.Max)
			st.Min = min(st.Min, stats.Min)
			st.Count += stats.Count
			st.Sum += stats.Sum
			stations[name] = st
		}
	}

	return stations, nil
}

func worker(chunks chan []string, results chan Stations) {

	for chunk := range chunks {

		s := make(Stations)
		for _, line := range chunk {
			parts := strings.Split(line, ";")
			name := parts[0]
			temp, _ := strconv.ParseFloat(parts[1], 64)

			st, ok := s[name]
			if !ok {
				st.Min = temp
				st.Max = temp
			}

			st.Max = max(st.Max, temp)
			st.Min = min(st.Min, temp)
			st.Count++
			st.Sum += temp
			s[name] = st
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

// String creates a string respresentation from stations map
func (s Stations) String() string {

	var sb strings.Builder
	sb.WriteString("{")

	for i, name := range s.sortNames() {
		if i > 0 {
			sb.WriteString(", ")
		}

		stats := s[name]
		statsStr := fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, stats.Min, stats.Sum/stats.Count, stats.Max)
		sb.WriteString(statsStr)
	}

	sb.WriteString("}")
	return sb.String()
}
