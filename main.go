package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	log.SetFlags(log.Ltime | log.Lshortfile)

	// Test Split Database Code
	_, err := splitDatabase("austen.db", "outputs", "output-%d.db", 50)
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	go func() {
		address := "localhost:8080"
		tempdir := filepath.Join("outputs")
		http.Handle("/outputs/", http.StripPrefix("/outputs", http.FileServer(http.Dir(tempdir))))
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Printf("Error in HTTP server for %s: %v", address, err)
		}
	}()

	var pathnames []string
	outputDir := "outputs"
	outputPattern := "output-%d.db"
	for i := 0; i < 50; i++ {
		url := "http://localhost:8080/"
		url = url + filepath.Join(outputDir, fmt.Sprintf(outputPattern, i))

		pathnames = append(pathnames, url)
	}
	_, err = mergeDatabases(pathnames, "testoutput.db", "temp.db")
	if err != nil {
		log.Fatalf("%v\n", err)
	}
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
		log.Fatalf("%v\n", err)
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
		log.Fatalf("%v\n", err)
		return nil, err
	}
	file.Close()

	// open database as file just created
	db, err := openDatabase(path)
	if err != nil {
		log.Fatalf("%v\n", err)
		return nil, err
	}
	_, err = db.Exec("create table pairs (key text, value text);")
	if err != nil {
		log.Fatalf("%v\n", err)
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
		log.Fatalf("%v\n", err)
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
			log.Fatalf("%v\n", err)
			return nil, err
		}
		dbs = append(dbs, tdb)
	}

	// create rows from input

	rows, err := db.Query("select key, value from pairs")
	if err != nil {
		log.Fatalf("%v\n", err)
		return nil, err
	}

	// iterate over rows
	index := 0
	total := 0
	for rows.Next() {
		if total%50 == 0 {
			fmt.Printf("%s row: %v\n", source, total)
		}
		// read the data using rows.Scan
		var key string
		var value string
		err = rows.Scan(&key, &value)
		if err != nil {
			log.Fatalf("%v\n", err)
			splitDatabaseCloser(db, dbs)
			return nil, err
		}

		// process the result
		_, err := dbs[index].Exec("insert into pairs (key, value) values (?,?)", key, value)
		if err != nil {
			log.Fatalf("%v\n", err)
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
		log.Fatalf("%v\n", err)
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
		log.Fatalf("%v\n", err)
		return nil, err
	}

	// for every url in urls, download the file and merge into db
	for i := 0; i < len(urls); i++ {
		fmt.Printf("downloading and merging: %s\n", urls[i])
		if err := download(urls[i], temp); err != nil {
			log.Fatalf("%v\n", err)
			return nil, err
		}
		if err := gatherInto(db, temp); err != nil {
			log.Fatalf("%v\n", err)
			return nil, err
		}
	}

	return db, nil
}

func download(url, path string) error {
	out, err := os.Create(path)
	if err != nil {
		log.Fatalf("%v\n", err)
		return err
	}
	defer out.Close()

	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("%v\n", err)
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
