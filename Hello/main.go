package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	"github.com/go-sql-driver/mysql"
)

type QueryResult struct {
	ID    int64
	Value string
}

type insertLine struct {
	title     string
	score     string
	id        string
	url       string
	commsNum  string
	created   string
	body      string
	timestamp string
}

var db *sql.DB

func main() {
	// Capture connection properties.
	cfg := mysql.Config{
		User:   "root",
		Passwd: "root",
		Net:    "tcp",
		Addr:   "localhost:3306",
		DBName: "bdii",
	}
	// Get a database handle.
	var err error
	db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}

	pingErr := db.Ping()
	if pingErr != nil {
		log.Fatal(pingErr)
	}
	fmt.Println("Connected!")
	albums, err := getRows(1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("IDs found: %v\n", albums)

	readCSV()
}

// albumsByArtist queries for albums that have the specified artist name.
func getRows(id int) ([]QueryResult, error) {
	// An albums slice to hold data from returned rows.
	var idResult []QueryResult

	rows, err := db.Query("SELECT * FROM teste WHERE id = ?", id)
	if err != nil {
		return nil, fmt.Errorf("ids %q: %v", id, err)
	}
	defer rows.Close()
	// Loop through rows, using Scan to assign column data to struct fields.
	for rows.Next() {
		var result QueryResult
		if err := rows.Scan(&result.ID, &result.Value); err != nil {
			return nil, fmt.Errorf("ids %q: %v", id, err)
		}
		idResult = append(idResult, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ids %q: %v", id, err)
	}
	return idResult, nil
}

func readCSV() {
	csvFile, err := os.Open("../db/reddit_vm.csv")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened CSV file")
	defer csvFile.Close()

	csvLines, err := csv.NewReader(csvFile).ReadAll()
	if err != nil {
		fmt.Println(err)
	}
	for _, line := range csvLines {
		emp := insertLine{
			title:     line[0],
			score:     line[1],
			id:        line[2],
			url:       line[3],
			commsNum:  line[4],
			created:   line[5],
			body:      line[6],
			timestamp: line[7],
		}
		fmt.Println(emp.title + " " + emp.id + " " + emp.timestamp + " done")
	}
}
