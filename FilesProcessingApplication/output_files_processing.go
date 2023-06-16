package main

import (
	"encoding/json"
	"fmt"
	"os"
)

var (
	outputDirectory = "./directory/output/"
)

func outputFilesProcessing(entity Entity) {

	outputFileName := outputDirectory + entity.UnitGuid + ".doc"

	fmt.Println("fileName: " + outputFileName)

	file, err := os.Create(outputFileName)
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	jsonData, err := json.Marshal(entity)
	if err != nil {
		fmt.Println(err)
	}

	file.WriteString(string(jsonData) + "\n")

	file.Close()

}
