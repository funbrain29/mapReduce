package mapreduce

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func master(client Interface, portNumber string, map_tasks string, reduce_tasks string, source_filename string) error {
	// collect arguments into int values
	MAP_TASKS, err := strconv.Atoi(map_tasks)
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	REDUCE_TASKS, err := strconv.Atoi(reduce_tasks)
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	PORT, err := strconv.Atoi(portNumber)
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	//setup tempdir and http server
	tempdir := filepath.Join(os.TempDir(), fmt.Sprintf("mapreduce.%d", os.Getpid()))
	os.RemoveAll(tempdir)
	os.Mkdir(tempdir, 0775)
	defer os.RemoveAll(tempdir)
	address := getLocalAddress(PORT)
	scanner := bufio.NewScanner(os.Stdin)

	actor := masterServer(address, PORT, tempdir)
	finished := make(chan struct{})
	actor <- func(f *Master) {
		finished <- struct{}{}
	}
	<-finished
	fmt.Printf("TEMP DIR: %s\n", tempdir)
	fmt.Printf("Starting Mapreduce. Splitting %s into %v map tasks and %v reduce tasks\n", source_filename, MAP_TASKS, REDUCE_TASKS)

	// split INPUT_FILE_NAME into MAP_TASKS files
	_, err = splitDatabase(source_filename, filepath.Join(tempdir), "map_%d_source.db", MAP_TASKS)
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	// create map tasks
	var mTasks []MapTask
	for i := 0; i < MAP_TASKS; i++ {
		mTasks = append(mTasks, MapTask{M: MAP_TASKS, R: REDUCE_TASKS, N: i, SourceHost: address})
	}
	var response LocalResponse
	var junk Nothing

	// send map tasks to actor
	actor.ExecuteMapTasks(mTasks, &junk)

	fmt.Printf("Executing map tasks, waiting for completion\n")
	// continue to ask until map tasks are finished
	actor.GetMapTaskFinished(junk, &response)
	for !response.TasksDone {
		time.Sleep(1 * time.Second)
		actor.GetMapTaskFinished(junk, &response)
	}
	addressList := response.AddressList

	// setup the ip addresses into http links to all of the output files
	var SourceFiles []string
	for i, v := range addressList {
		for j := 0; j < REDUCE_TASKS; j++ {
			SourceFiles = append(SourceFiles, makeURL(v, mapOutputFile(i, j)))
		}
	}

	// divide addressList into REDUCETASK parts
	var divided [][]string
	chunkSize := len(SourceFiles) / REDUCE_TASKS
	for i := 0; i < len(SourceFiles); i += chunkSize {
		end := i + chunkSize

		if end > len(SourceFiles) {
			end = len(SourceFiles)
		}
		divided = append(divided, SourceFiles[i:end])
	}

	// create list of reduce tasks
	var rTasks []ReduceTask
	for i := 0; i < REDUCE_TASKS; i++ {
		rTasks = append(rTasks, ReduceTask{M: MAP_TASKS, R: REDUCE_TASKS, N: i, SourceHosts: divided[i]})
	}
	actor.ExecuteReduceTasks(rTasks, &junk)
	fmt.Printf("Executing Reduce tasks, waiting for completion\n")
	actor.GetReduceTaskFinished(junk, &response)
	for !response.TasksDone {
		time.Sleep(1 * time.Second)
		actor.GetReduceTaskFinished(junk, &response)
	}

	addressList = response.AddressList

	fmt.Printf("All MapReduce work done, merging output file\n")
	// merge reduce files back to one file
	var mergeList []string
	for i, v := range addressList {
		mergeList = append(mergeList, makeURL(v, reduceOutputFile(i)))
	}
	outputFileName := "ResultsOf-" + source_filename
	mergeDatabases(mergeList, outputFileName, filepath.Join(tempdir, "temp.db"))

	// after merging, shutdown any workers and wait a moment to ensure they close
	actor.Shutdown(junk, &junk)
	time.Sleep(1 * time.Second)

	// Stall for user input before quitting and deleting temp files
	fmt.Printf("'%s' created. Press enter to delete all temp data in: %s", outputFileName, tempdir)
	scanner.Scan()

	return nil
}

func worker(client Interface, portNumber string, masterPort string) error {
	// collect arguments into int values
	masterPortNumber, err := strconv.Atoi(masterPort)
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	PORT, err := strconv.Atoi(portNumber)
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	//setup tempdir and http server
	tempdir := filepath.Join(os.TempDir(), fmt.Sprintf("mapreduce.%d", os.Getpid()))
	os.RemoveAll(tempdir)
	os.Mkdir(tempdir, 0775)
	defer os.RemoveAll(tempdir)
	address := getLocalAddress(PORT)
	maddress := getLocalAddress(masterPortNumber)

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", PORT))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
	go http.Serve(l, nil)

	fmt.Printf("Started fileserver with address: %s\n", address)
	fmt.Printf("TEMP DIR: %s\n", tempdir)
	fmt.Printf("Waiting for work...\n")

	for {
		var response Response
		err = callErr(maddress, "Server.GetWork", &address, &response)
		if err != nil {
			log.Fatalf("%v\n", err)
		}
		if response.WorkType != 0 { // If there is work to do
			if response.WorkType == 1 { // map
				response.Maptask.Process(tempdir, client)
			} else if response.WorkType == 2 { // reduce
				response.Reducetask.Process(tempdir, client)
			}
			var response Response
			err = callErr(maddress, "Server.FinishedWork", &address, &response)
			if err != nil {
				log.Fatalf("%v\n", err)
			}

		} else if response.Shutdown { // If no work, check if shutting down
			fmt.Printf("Master indicated Mapreduce job completed, deleting temp files and closing program.\n")
			return nil
		}
		// sleep a second inbetween requests
		time.Sleep(1 * time.Second)
	}
}

func Start(client Interface, dir string, INPUT_FILE_NAME string) error {
	log.SetFlags(log.Ltime | log.Lshortfile)
	runtime.GOMAXPROCS(1)

	// get argument data from command line
	args := os.Args[1:]

	if len(args) == 2 { //worker
		return worker(client, args[0], args[1])
	} else if len(args) == 3 { //master
		return master(client, args[0], args[1], args[2], INPUT_FILE_NAME)
	} else { // throw error
		log.Fatalf("\nPlease supply arguments for one of the following:\nMaster Node: [PortNumber, NumberOfMapTasks, NumberOfReduceTasks]\nWorker Node: [PortNumber, MasterPortNumber]\n")
	}
	return nil
}

func openDatabase(path string) (*sql.DB, error) {
	options :=
		"?" + "_busy_timeout=10000" +
			"&" + "_case_sensitive_like=OFF" +
			"&" + "_foreign_keys=ON" +
			"&" + "_journal_mode=OFF" +
			"&" + "_locking_mode=NORMAL" +
			"&" + "mode=rw" +
			"&" + "_synchronous=OFF"
	db, err := sql.Open("sqlite3", path+options)
	if err != nil {
		log.Fatalf("%v", err)
		return nil, err
	}
	return db, nil
}

func createDatabase(path string) (*sql.DB, error) {
	// delete file
	err := os.Remove(path)
	if err == nil {
		fmt.Printf("deleted existing file '%s'\n", path)
	}

	// create file
	file, err := os.Create(path)
	if err != nil {
		log.Fatalf("%v", err)
		return nil, err
	}
	file.Close()

	// open database as file just created
	db, err := openDatabase(path)
	if err != nil {
		log.Fatalf("%v", err)
		return nil, err
	}
	_, err = db.Exec("create table pairs (key text, value text);")
	if err != nil {
		log.Fatalf("%v", err)
		db.Close()
		return nil, err
	}
	return db, nil
}

// paths, err := splitDatabase("input.db", "outputs", "output-%d.db", 50)
func splitDatabase(source, outputDir, outputPattern string, m int) ([]string, error) {
	// open input database
	db, err := openDatabase(source)
	if err != nil {
		log.Fatalf("%v", err)
		return nil, err
	}
	defer db.Close()

	// create databases slice
	var dbs []*sql.DB
	var pathnames []string
	for i := 0; i < m; i++ {
		// dbfile will look like outputs/output-[i].db where outputDir = "output" and outputPatter = "output-%d.db"
		dbfile := filepath.Join(outputDir, fmt.Sprintf(outputPattern, i))
		pathnames = append(pathnames, dbfile)
		tdb, err := createDatabase(dbfile)
		if err != nil {
			log.Fatalf("%v", err)
			return nil, err
		}
		dbs = append(dbs, tdb)
	}

	// get total amount of rows
	var totalRows int
	log.Printf("source: %s\n", source)
	rows, err := db.Query("select count(1) from pairs")
	if err != nil {
		log.Fatalf("%v", err)
		return nil, err
	}
	for rows.Next() {
		var count string
		err = rows.Scan(&count)
		if err != nil {
			log.Fatalf("%v", err)
			splitDatabaseCloser(db, dbs)
			return nil, err
		}
		totalRows, err = strconv.Atoi(count)
		if err != nil {
			log.Fatalf("%v", err)
			splitDatabaseCloser(db, dbs)
			return nil, err
		}
	}

	// creating 10 percent of total rows to check against total
	totalRows10 := totalRows / 10

	// create rows from input
	rows, err = db.Query("select key, value from pairs")
	if err != nil {
		log.Fatalf("%v", err)
		return nil, err
	}

	// iterate over rows
	index := 0
	total := 0
	percent := 0
	for rows.Next() {
		if total%totalRows10 == 0 {
			printpercent := strconv.Itoa(percent)
			fmt.Printf("Splitting %s row: %s\n", source, printpercent+"%")
			percent = percent + 10
		}
		// read the data using rows.Scan
		var key string
		var value string
		err = rows.Scan(&key, &value)
		if err != nil {
			log.Fatalf("%v", err)
			splitDatabaseCloser(db, dbs)
			return nil, err
		}

		// process the result
		_, err := dbs[index].Exec("insert into pairs (key, value) values (?,?)", key, value)
		if err != nil {
			log.Fatalf("%v", err)
			splitDatabaseCloser(db, dbs)
			return nil, err
		}

		// increment index up to m
		index++
		total++
		if index >= m {
			index = 0
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("%v", err)
		splitDatabaseCloser(db, dbs)
		return nil, err
	}

	// close databases
	splitDatabaseCloser(db, dbs)
	if total < m {
		return nil, fmt.Errorf("splitDatabase, less values in input than output files.\n")
	}

	// return slice of pathnames
	return pathnames, nil
}

// Helper function for splitDatabase, closes all open databases
func splitDatabaseCloser(db *sql.DB, dbs []*sql.DB) {
	db.Close()
	for i := 0; i < len(dbs); i++ {
		dbs[i].Close()
	}
}

func mergeDatabases(urls []string, path string, temp string) (*sql.DB, error) {
	// open new database with path
	db, err := createDatabase(path)
	if err != nil {
		log.Fatalf("%v", err)
		return nil, err
	}

	// for every url in urls, download the file and merge into db
	for i := 0; i < len(urls); i++ {
		if err := download(urls[i], temp); err != nil {
			log.Fatalf("%v", err)
			return nil, err
		}
		if err := gatherInto(db, temp); err != nil {
			log.Fatalf("%v", err)
			return nil, err
		}
	}

	return db, nil
}

func download(url, path string) error {
	out, err := os.Create(path)
	if err != nil {
		log.Fatalf("%v", err)
		return err
	}
	defer out.Close()

	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("%v", err)
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(out, resp.Body)

	return err
}

func gatherInto(db *sql.DB, path string) error {
	if _, err := db.Exec("attach ? as merge;", path); err != nil {
		return err
	}
	if _, err := db.Exec("insert into pairs select * from merge.pairs;"); err != nil {
		return err
	}
	if _, err := db.Exec("detach merge;"); err != nil {
		return err
	}

	err := os.Remove(path)
	return err
}
