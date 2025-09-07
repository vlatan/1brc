package main

import (
	"bufio"
	"fmt"
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
			name, temp := parseLine(line)

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

func parseLine(line string) (string, int64) {
	for i, char := range line {
		if char == ';' {
			return line[:i], parseTemp(line[i+1:])
		}
	}

	return "", 0
}

func parseTemp(temp string) (result int64) {
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
	}

	if neg {
		return -result
	}

	return
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
