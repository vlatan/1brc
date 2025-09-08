# Golang implementation of 1BRC

Inspired by: https://github.com/shraddhaag/1brc

## Instructions

Clone the [The One Billion Row Challenge](https://github.com/gunnarmorling/1brc) repo and cd into it.

### Generate the input file:
```
docker run --rm -v $(pwd):/workspace -w /workspace openjdk:21-jdk \
javac -d . src/main/java/dev/morling/onebrc/CreateMeasurements.java
```

```
docker run --rm -v $(pwd):/workspace -w /workspace openjdk:21-jdk \
java dev.morling.onebrc.CreateMeasurements 1000000000
```

### Build and run:

Copy over the generated file to a desired location and supply its path to this binary.

``` bash
go build main.go
./main -f measurements.txt
```