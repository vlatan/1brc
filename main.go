package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
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

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("couldn't open the file: %v", err)
	}

	defer file.Close()

	stations := make(Stations)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ";")
		name := parts[0]

		temp, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return nil, fmt.Errorf("couldn't convert string to float: %v", err)
		}

		st, ok := stations[name]
		if !ok {
			st.Min = temp
			st.Max = temp
		}

		if temp > st.Max {
			st.Max = temp
		} else if temp < st.Min {
			st.Min = temp
		}

		st.Count++
		st.Sum += temp
		stations[name] = st
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading the file: %v", err)
	}

	return stations, nil
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
