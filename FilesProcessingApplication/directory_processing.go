package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {

	//TODO: clear real parameters in the config file
	DBHost      string `json:"host"`
	DBPort      int    `json:"port"`
	DBUsername  string `json:"username"`
	DBPassword  string `json:"password"`
	DBName      string `json:"name"`
	DBDirectory string `json:"directory"`
}

type Entity struct {
	N         int    `json:"n"`
	Mqtt      string `json:"mqtt"`
	Invid     string `json:"invid"`
	UnitGuid  string `json:"unit_guid"`
	MsgId     string `json:"msg_id"`
	Text      string `json:"text"`
	Context   string `json:"context"`
	Class     string `json:"class"`
	Level     int    `json:"level"`
	Area      string `json:"area"`
	Addr      string `json:"addr"`
	Block     bool   `json:"block"`
	Type      string `json:"type"`
	Bit       int    `json:"bit"`
	InvertBit int    `json:"invert_bit"`
}

type File struct {
	Path         string
	LastModified time.Time
}

var (
	db            *sql.DB
	queue         []File
	dirPath       = "./directory/input"
	dbTable       = "platforms"
	dbCreateQuery = "CREATE TABLE IF NOT EXISTS PLATFORMS (n integer PRIMARY KEY, mqtt VARCHAR(50), invid VARCHAR(50), " +
		"unit_guid VARCHAR(70), msg_id VARCHAR(70), text VARCHAR(70), context VARCHAR(70), class VARCHAR(50), level INTEGER, " +
		"area VARCHAR(50), addr VARCHAR(70), block BOOLEAN, type VARCHAR(50), bit INTEGER, invert_bit INTEGER)"
	dbInsertQuery = "INSERT INTO " + dbTable + " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
	dbSelectQuery = "SELECT COUNT(*) FROM " + dbTable + " WHERE n = %s AND mqtt = %s AND invid = %s AND unit_guid = %s AND msg_id = %s AND text = %s" +
		" AND context = %s AND class = %s AND level = %s AND area = %s AND addr = %s AND block = %s AND type = %s AND bit = %s AND invert_bit = %s"
	dbUpdateQuery = "UPDATE " + dbTable + " SET mqtt = %s AND invid = %s AND unit_guid = %s AND msg_id = %s AND text = %s AND context = %s AND class = %s AND level = %s " +
		" AND area = %s AND addr = %s AND block = %s AND type = %s AND bit = %s AND invert_bit = %s WHERE n = %s"
	extension     = ".tsv"
	checkInterval = 10 * time.Second
	maxQueueSize  = 10
	maxFiles      = 5
)

func main() {

	var err error

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
	db, err = sql.Open("postgres", dbinfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}

	err = createTable()
	if err != nil {
		log.Fatal(err)
	}

	go apiGettingDataProcessing()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			files, err := getFiles(dirPath)
			if err != nil {
				fmt.Println(err)
				continue
			}

			for _, file := range files {
				if filepath.Ext(file.Path) == extension {
					queueFile(file)
				}
			}

			if len(queue) > 0 {
				processQueue()
			}
		}
	}
}

func getFiles(dirPath string) ([]File, error) {
	var files []File

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			files = append(files, File{Path: path, LastModified: info.ModTime()})
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

func queueFile(file File) {

	if !isInQueue(file) {
		if len(queue) == maxQueueSize {
			queue = queue[1:]
		}

		queue = append(queue, file)
	}
}

func isInQueue(file File) bool {
	for _, f := range queue {
		if f.Path == file.Path && f.LastModified.Equal(file.LastModified) {
			return true
		}
	}

	return false
}

func processQueue() {
	filesNumber := len(queue)
	if filesNumber > maxFiles {
		filesNumber = maxFiles
	}

	for i := 0; i < filesNumber; i++ {
		go func() {
			file := queue[0]
			queue = queue[1:]

			if err := processFile(file); err != nil {
				fmt.Println(err)
			}
		}()
	}
}

func processFile(file File) error {
	linesCounter := 1

	f, err := os.Open(file.Path)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		fields := strings.Split(line, "\t")

		for i := 0; i < 15; i++ {
			fields[i] = strings.TrimSpace(fields[i])
			if fields[i] == "" {
				fields[i] = "null"
			} else {
				if _, err := strconv.Atoi(fields[i]); err != nil {
					fields[i] = "'" + fields[i] + "'"
				}
			}
		}

		n := fields[0]
		mqtt := fields[1]
		invid := fields[2]
		unit_guid := fields[3]
		msg_id := fields[4]
		text := fields[5]
		context := fields[6]
		class := fields[7]
		level := fields[8]
		area := fields[9]
		addr := fields[10]
		block := fields[11]
		type_table := fields[12]
		bit := fields[13]
		invert_bit := fields[14]

		nInt, err := strconv.Atoi(n)
		if err != nil {
			fmt.Println(err)
		}
		levelInt, err := strconv.Atoi(level)
		if err != nil {
			fmt.Println(err)
		}
		blockBool, err := strconv.ParseBool(block)
		if err != nil {
			fmt.Println(err)
		}
		bitInt, err := strconv.Atoi(bit)
		if err != nil {
			fmt.Println(err)
		}
		invertBitInt, err := strconv.Atoi(invert_bit)
		if err != nil {
			fmt.Println(err)
		}

		entity := Entity{
			UnitGuid:  unit_guid,
			N:         nInt,
			Mqtt:      mqtt,
			Invid:     invid,
			MsgId:     msg_id,
			Text:      text,
			Context:   context,
			Class:     class,
			Level:     levelInt,
			Area:      area,
			Addr:      addr,
			Block:     blockBool,
			Type:      type_table,
			Bit:       bitInt,
			InvertBit: invertBitInt,
		}

		if linesCounter != 1 {
			outputFilesProcessing(entity)
			if existsInDB(n, mqtt, invid, unit_guid, msg_id, text, context, class, level, area, addr, block, type_table, bit, invert_bit) {
				if err := updateInDB(n, mqtt, invid, unit_guid, msg_id, text, context, class, level, area, addr, block, type_table, bit, invert_bit); err != nil {
					return err
				}
			} else {
				if err := insertInDB(n, mqtt, invid, unit_guid, msg_id, text, context, class, level, area, addr, block, type_table, bit, invert_bit); err != nil {
					return err
				}
			}
		}
		linesCounter++
	}

	return nil
}
