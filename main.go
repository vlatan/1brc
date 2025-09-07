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
	Min, Max, Count, Sum int64
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

	chunkSize := 5_000_000 // Lines in a chunk
	numWorkers := runtime.NumCPU() - 1
	results := make(chan Stations)
	chunks := make(chan []string, 10)
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
				chunk = chunk[:0]
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

func worker(chunks chan []string, results chan Stations) {

	for chunk := range chunks {

		s := make(Stations)
		for _, line := range chunk {
			name, tempStr, _ := strings.Cut(line, ";")
			temp := parseTemp(tempStr)

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

func parseTemp(temp string) int64 {
	temp = strings.Replace(temp, ".", "", 1)
	result, _ := strconv.ParseInt(temp, 10, 64)
	return result
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
		minTemp := float64(stats.Min) / 10.0
		maxTemp := float64(stats.Max) / 10.0
		avgTemp := float64(stats.Sum) / float64(stats.Count) / 10.0
		statsStr := fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, minTemp, avgTemp, maxTemp)
		sb.WriteString(statsStr)
	}

	sb.WriteString("}")
	return sb.String()
}
