package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"strconv"
)

var (
	selectAPITemplate = "SELECT * FROM PLATFORMS WHERE n>= %d AND n<=%d AND unit_guid='%s'"
)

func apiGettingDataProcessing() {
	r := mux.NewRouter()
	r.HandleFunc("/data", getData).Methods("GET")
	log.Fatal(http.ListenAndServe(":8080", r))
}

func getData(w http.ResponseWriter, r *http.Request) {

	query := r.URL.Query()
	page, err := strconv.Atoi(query.Get("page"))
	if err != nil {
		page = 1
	}
	limit, err := strconv.Atoi(query.Get("limit"))
	if err != nil {
		limit = 20
	}
	unitGUID := query.Get("unitGUID")

	minValue := (page-1)*limit + 1
	maxValue := page * limit

	file, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatal(err)
	}

	var config Config
	err = json.Unmarshal(file, &config)
	if err != nil {
		log.Fatal(err)
	}

	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", config.DBUsername, config.DBPassword, config.DBName)
	db, err := sql.Open("postgres", dbinfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sql := fmt.Sprintf(selectAPITemplate, minValue, maxValue, unitGUID)
	fmt.Println(sql)
	var result string
	err = db.QueryRow(sql).Scan(&result)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Fprintln(w, result)

}
