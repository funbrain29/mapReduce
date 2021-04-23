package main

import (
	"log"
	"mapreduce"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

func main() {
	var c Client
	INPUT_FILE_NAME := "austen.db"

	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	if err := mapreduce.Start(c, dir, INPUT_FILE_NAME); err != nil {
		log.Fatalf("%v", err)
	}
}

// Map and Reduce functions for a basic wordcount client

type Client struct{}

func (c Client) Map(key, value string, output chan<- mapreduce.Pair) error {
	defer close(output)
	lst := strings.Fields(value)
	for _, elt := range lst {
		word := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, elt)
		if len(word) > 0 {
			output <- mapreduce.Pair{Key: word, Value: "1"}
		}
	}
	return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- mapreduce.Pair) error {
	defer close(output)
	count := 0
	for v := range values {
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}
	p := mapreduce.Pair{Key: key, Value: strconv.Itoa(count)}
	output <- p
	return nil
}
