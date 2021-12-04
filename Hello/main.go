package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
)

// type QueryResult struct {
// 	ID    int64
// 	Value string
// }

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
		DBName: "bdii_tp1",
	}

	// Get a database handle.
	var err error
	db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}

	//db.SetMaxOpenConns(1)

	defer db.Close()
	//db.SetConnMaxLifetime(20)

	// pingErr := db.Ping()
	// if pingErr != nil {
	// 	log.Fatal(pingErr)
	// }
	// fmt.Println("Connected!")
	// albums, err := getRows(1)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("IDs found: %v\n", albums)
	fmt.Println(time.Now())
	explicitInsert()
	fmt.Println(time.Now())
	implicitInsert()
	fmt.Println(time.Now())
	//readCSV()
	deleteAll()
}

// // albumsByArtist queries for albums that have the specified artist name.
// func getRows(id int) ([]QueryResult, error) {
// 	// An albums slice to hold data from returned rows.
// 	var idResult []QueryResult

// 	rows, err := db.Query("SELECT * FROM arquivo WHERE id = ?", id)
// 	if err != nil {
// 		return nil, fmt.Errorf("ids %q: %v", id, err)
// 	}
// 	defer rows.Close()
// 	// Loop through rows, using Scan to assign column data to struct fields.
// 	for rows.Next() {
// 		var result QueryResult
// 		if err := rows.Scan(&result.ID, &result.Value); err != nil {
// 			return nil, fmt.Errorf("ids %q: %v", id, err)
// 		}
// 		idResult = append(idResult, result)
// 	}
// 	if err := rows.Err(); err != nil {
// 		return nil, fmt.Errorf("ids %q: %v", id, err)
// 	}
// 	return idResult, nil
// }

func explicitInsert() {
	insertNewLine, err := db.Prepare("INSERT INTO arquivo(title, score, id, url, comms_num, created, body, timestamp) VALUES (?,?,?,?,?,?,?,?)")
	if err != nil {
		return
	}
	csvFile, err := os.Open("../db/reddit_vm.csv")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened CSV file")
	defer csvFile.Close()

	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	csvLines, err := csv.NewReader(csvFile).ReadAll()
	if err != nil {
		fmt.Println(err)
	}
	for _, line := range csvLines {
		Result := insertLine{
			title:     line[0],
			score:     line[1],
			id:        line[2],
			url:       line[3],
			commsNum:  line[4],
			created:   line[5],
			body:      line[6],
			timestamp: line[7],
		}

		tx.Stmt(insertNewLine).Exec(Result.title, Result.score, Result.id, Result.url, Result.commsNum, Result.created, Result.body, Result.timestamp)

	}
	tx.Commit()

}

func implicitInsert() {
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
		Result := insertLine{
			title:     line[0],
			score:     line[1],
			id:        line[2],
			url:       line[3],
			commsNum:  line[4],
			created:   line[5],
			body:      line[6],
			timestamp: line[7],
		}

		rows, err := db.Query("INSERT INTO arquivo(title, score, id, url, comms_num, created, body, timestamp) VALUES (?,?,?,?,?,?,?,?)", Result.title, Result.score, Result.id, Result.url, Result.commsNum, Result.created, Result.body, Result.timestamp)
		if err != nil {
			fmt.Println(err)
		} else {
			rows.Close()
		}
		//fmt.Println(Result.id + " inserted")
	}
}

// func readCSV() []insertLine {
// 	results := []*insertLine{}
// 	f, err := os.Open("../db/reddit_vm.csv")
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	defer f.Close()

// 	// Read File into a Variable
// 	// results, err := csv.NewReader(f).ReadAll()
// 	return results
// }

func deleteAll() {
	rows, err := db.Query("DELETE FROM arquivo WHERE 1=1")
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()
}
