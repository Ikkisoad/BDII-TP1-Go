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

	fmt.Println(time.Now())
	explicitInsert()
	fmt.Println(time.Now())
	implicitInsert()
	fmt.Println(time.Now())
	deleteAll()
}

func explicitInsert() {
	insertNewLine, err := db.Prepare("INSERT INTO arquivo(title, score, id, url, comms_num, created, body, timestamp) VALUES (?,?,?,?,?,?,?,?)")
	if err != nil {
		return
	}
	csvFile, err := os.Open("../db/reddit_vm.csv")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Explicit:")
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
	fmt.Println("Implicit:")
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

func deleteAll() {
	rows, err := db.Query("DELETE FROM arquivo WHERE 1=1")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("All rows deleted")
	defer rows.Close()
}
