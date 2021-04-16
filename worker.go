package main

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

type MapTask struct {
	M, R       int    // total number of map and reduce tasks
	N          int    // map task number, 0-based
	SourceHost string // address of host with map input file
}

type ReduceTask struct {
	M, R        int      // total number of map and reduce tasks
	N           int      // reduce task number, 0-based
	SourceHosts []string // addresses of map workers
}

type Pair struct {
	Key   string
	Value string
}

type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

func mapSourceFile(m int) string       { return fmt.Sprintf("map_%d_source.db", m) }
func mapInputFile(m int) string        { return fmt.Sprintf("map_%d_input.db", m) }
func mapOutputFile(m, r int) string    { return fmt.Sprintf("map_%d_output_%d.db", m, r) }
func reduceInputFile(r int) string     { return fmt.Sprintf("reduce_%d_input.db", r) }
func reduceOutputFile(r int) string    { return fmt.Sprintf("reduce_%d_output.db", r) }
func reducePartialFile(r int) string   { return fmt.Sprintf("reduce_%d_partial.db", r) }
func reduceTempFile(r int) string      { return fmt.Sprintf("reduce_%d_temp.db", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }

// create R output files !

// when calling client.Map, you should be spinning up a new go rutine before calling client.Map.
// This helper will wait for channel items to come out of the client.Map channel and will add them to the correct database.
// before starting the helper, create another channel that will tell the helper when client.Map is finished

func (task *MapTask) Process(tempdir string, client Interface) error {
	fmt.Printf("Processing MapTask #%d\n", task.N)

	// download Source file as the Input File
	download(makeURL(task.SourceHost, mapSourceFile(task.N)), filepath.Join(tempdir, mapInputFile(task.N)))

	// Split the Input file into many Output files
	var dbs []*sql.DB
	for i := 0; i < task.R; i++ {
		dbfile := filepath.Join(tempdir, mapOutputFile(task.N, i))
		tdb, err := createDatabase(dbfile)
		if err != nil {
			log.Fatalf("Error processing Maptask: %v\n", err)
			return err
		}
		dbs = append(dbs, tdb)
	}

	// Open the source file
	sourceDb, err := openDatabase(filepath.Join(tempdir, mapSourceFile(task.N)))
	if err != nil {
		log.Fatalf("Error processing Maptask: %v\n", err)
		return err
	}
	// pull pairs from source file
	rows, err := sourceDb.Query("select key, value from pairs")
	if err != nil {
		log.Fatalf("Error processing Maptask: %v\n", err)
		return err
	}

	// loop over every pair
	for rows.Next() {
		// put the pair into a Pair object
		var key string
		var value string
		err = rows.Scan(&key, &value)
		if err != nil {
			log.Fatalf("Error processing Maptask: %v\n", err)
			return err
		}

		// pass them into a client.Map function
		// we call a goroutine to handle the channel while the main routine handles the client.Map function
		pairChan := make(chan Pair, 100)
		if err != nil {
			log.Fatalf("Error processing Maptask: %v\n", err)
			return err
		}
		// goroutine to get the pair from the channel and insert it into the correct output file
		finished := make(chan struct{})
		go func() {
			for pair := range pairChan {
				hash := fnv.New32() // from the stdlib package hash/fnv
				hash.Write([]byte(pair.Key))
				index := int(hash.Sum32() % uint32(task.R)) // index is the output file this pair should go in
				_, err := dbs[index].Exec("insert into pairs (key, value) values (?,?)", pair.Key, pair.Value)
				if err != nil {
					log.Fatalf("Error processing Maptask: %v\n", err)
				}
			}
			// push a value into the finished channel to tell the main loop it can continue
			finished <- struct{}{}
		}()
		// call client.Map on the key value, the goroutine above will process the result and add it to the correct output
		// Map(key, value string, output chan<- Pair) error
		err = client.Map(key, value, pairChan)
		if err != nil {
			log.Fatalf("Error processing Maptask: %v\n", err)
			return err
		}
		// pause until the worker has finished inserting keys. aka wait for a value from finished
		<-finished
		close(finished)
	}

	//close open databases
	for i := 0; i < len(dbs); i++ {
		dbs[i].Close()
	}
	sourceDb.Close()

	return nil
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	fmt.Printf("Processing ReduceTask #%d\n", task.N)
	// Download and merge all map inputs into a Tempfile
	mergeDatabases(task.SourceHosts, filepath.Join(tempdir, reduceInputFile(task.N)), filepath.Join(tempdir, reduceTempFile(task.N)))

	// create the input and output files
	dbfile := filepath.Join(tempdir, reduceInputFile(task.N))
	inputDb, err := openDatabase(dbfile)
	if err != nil {
		log.Fatalf("Error processing Reducetask: %v\n", err)
		return nil
	}

	dbfile = filepath.Join(tempdir, reduceOutputFile(task.N))
	outputDb, err := createDatabase(dbfile)
	if err != nil {
		log.Fatalf("Error processing Reducetask: %v\n", err)
		return nil
	}

	// query the input file, getting keys and values in order
	rows, err := inputDb.Query("select key, value from pairs order by key, value")
	if err != nil {
		log.Fatalf("Error processing Reducetask: %v\n", err)
		return err
	}

	//setup variables for the main reduceloop
	var previousKey string
	valuesChan := make(chan string, 100)
	complete := make(chan struct{})
	firstrun := true

	// this loop is in charge of feeding input to and managing the sub routines running client.Reduce
	for rows.Next() {
		// put the pair into a Pair object
		var key string
		var value string
		err = rows.Scan(&key, &value)
		if err != nil {
			log.Fatalf("Error processing Reducetask: %v\n", err)
		}
		// feed the pair into input
		// check if its the first run of the loop
		if firstrun {
			firstrun = false
			previousKey = key
			go reduceRoutines(key, outputDb, client, valuesChan, complete)
		}

		// feed a value to the reduce routines every loop
		valuesChan <- value

		// compare new key to last key, if new key, close values channel
		if key != previousKey {
			previousKey = key
			// close value channel, telling cleint.Reduce to finish
			close(valuesChan)
			// wait until reduce loop begins again
			<-complete

			valuesChan = make(chan string, 100)
			go reduceRoutines(key, outputDb, client, valuesChan, complete)
		}
	}
	//out of keys, clean up loop
	close(valuesChan)
	<-complete

	// close open databases
	outputDb.Close()
	inputDb.Close()

	return nil
}

// runs two goroutines, runs client.reduce and puts the output into a dabatase,
// runs until valuesChan is closed, sends a struct through complete when finished
func reduceRoutines(key string, outputDb *sql.DB, client Interface, valuesChan chan string, complete chan struct{}) {

	// goroutine that loops over the output channel, taking values until its closed by client.Reduce
	outputChan := make(chan Pair, 100)
	finished := make(chan struct{})
	go func() {
		for pair := range outputChan {
			_, err := outputDb.Exec("insert into pairs (key, value) values (?,?)", pair.Key, pair.Value)
			if err != nil {
				log.Fatalf("Error processing Reducetask: %v\n", err)
			}
		}
		finished <- struct{}{}
	}()

	// Reduce(key string, values <-chan string, output chan<- Pair) error
	// Reduce will run until the values channel is closed, it will then close the output channel
	err := client.Reduce(key, valuesChan, outputChan)
	if err != nil {
		log.Fatalf("Error processing Reducetask: %v\n", err)
	}
	// ensure that client.reduce has finished, and the outputchan goroutine is finished
	<-finished
	close(finished)
	complete <- struct{}{}
}
