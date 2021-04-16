package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"unicode"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	log.SetFlags(log.Ltime | log.Lshortfile)
	runtime.GOMAXPROCS(1)

	MAP_TASKS := 9
	REDUCE_TASKS := 3
	INPUT_FILE_NAME := "austen.db"
	scanner := bufio.NewScanner(os.Stdin)

	//setup tempdir and http server
	tempdir := filepath.Join(os.TempDir(), fmt.Sprintf("mapreduce.%d", os.Getpid()))
	os.RemoveAll(tempdir)
	os.Mkdir(tempdir, 0775)
	defer os.RemoveAll(tempdir)
	address := "localhost:3410"
	go func() {
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Printf("Error in HTTP server for %s: %v", address, err)
		}
	}()
	fmt.Printf("TEMP DIR: %s\n", tempdir)
	fmt.Printf("HTTP Server running, Press enter to begin MapReduce")
	scanner.Scan()

	// create map tasks
	var mTasks []MapTask
	for i := 0; i < MAP_TASKS; i++ {
		mTasks = append(mTasks, MapTask{M: 9, R: 3, N: i, SourceHost: filepath.Join(address, "data")})
	}

	// create addressList for reduce tasks
	var addressList []string
	for i := 0; i < REDUCE_TASKS; i++ {
		for j := 0; j < MAP_TASKS; j++ {
			addressList = append(addressList, "http://"+filepath.Join(address, "data", fmt.Sprintf("map_%d_output_%d.db", j, i)))
		}
	}

	// divide addressList into REDUCETASK parts
	var divided [][]string
	chunkSize := len(addressList) / REDUCE_TASKS
	for i := 0; i < len(addressList); i += chunkSize {
		end := i + chunkSize

		if end > len(addressList) {
			end = len(addressList)
		}

		divided = append(divided, addressList[i:end])
	}

	// create list of reduce tasks
	var rTasks []ReduceTask
	for i := 0; i < REDUCE_TASKS; i++ {
		rTasks = append(rTasks, ReduceTask{M: 9, R: 3, N: i, SourceHosts: divided[i]})
	}

	// split INPUT_FILE_NAME into MAP_TASKS files
	_, err := splitDatabase(INPUT_FILE_NAME, filepath.Join(tempdir), "map_%d_source.db", MAP_TASKS)
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	// create client object with Map and Reduce functions
	client := Client{}

	// run all map tasks
	for _, v := range mTasks {
		v.Process(tempdir, client)
	}
	//run all reduce tasks
	for _, v := range rTasks {
		v.Process(tempdir, client)
	}
	fmt.Printf("Reduce tasks completed\n")

	// merge reduce files back to one file
	var mergeList []string
	for i := 0; i < REDUCE_TASKS; i++ {
		mergeList = append(mergeList, "http://"+filepath.Join(address, "data", fmt.Sprintf("reduce_%d_output.db", i)))
	}
	outputFileName := "ResultsOf-" + INPUT_FILE_NAME
	mergeDatabases(mergeList, outputFileName, filepath.Join(tempdir, "temp.db"))

	// Stall for user input before quitting and deleting temp files
	fmt.Printf("'%s' created. Press enter to delete all temp data in: %s", outputFileName, tempdir)
	scanner.Scan()
}

type Client struct{}

func (c Client) Map(key, value string, output chan<- Pair) error {
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
			output <- Pair{Key: word, Value: "1"}
		}
	}
	return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- Pair) error {
	defer close(output)
	count := 0
	for v := range values {
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}
	p := Pair{Key: key, Value: strconv.Itoa(count)}
	output <- p
	return nil
}

// func mapSourceFile(m int) string { return fmt.Sprintf("map_%d_source.db", m) }

func testSplitAndMerge() {
	log.SetFlags(log.Ltime | log.Lshortfile)

	// Test Split Database Code
	_, err := splitDatabase("austen.db", "outputs", "output-%d.db", 50)
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	go func() {
		address := "localhost:8080"
		tempdir := filepath.Join("outputs")
		http.Handle("/outputs/", http.StripPrefix("/outputs",
			http.FileServer(http.Dir(tempdir))))
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Printf("Error in HTTP server for %s: %v", address, err)
		}
	}()

	fmt.Printf("Splitting Complete, HTTP server running. Press enter to start Merging")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()

	// Test Merge Database Code
	outputDir := "outputs"
	outputPattern := "output-%d.db"

	var pathnames []string
	for i := 0; i < 50; i++ {
		url := "http://localhost:8080/"
		url = url + filepath.Join(outputDir, fmt.Sprintf(outputPattern, i))

		pathnames = append(pathnames, url)
	}
	_, err = mergeDatabases(pathnames, "austenRebuilt.db", "temp.db")
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	fmt.Printf("Split and Merge Complete, Press enter to close HTTP server")
	scanner.Scan()

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
		fmt.Printf("downloading and merging: %s\n", urls[i])
		if err := download(urls[i], temp); err != nil {
			log.Fatalf("%v", err)
			return nil, err
		}
		if err := gatherInto(db, temp); err != nil {
			log.Fatalf("%v", err)
			return nil, err
		}
	}
	fmt.Printf("Merging complete\n")

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
