package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer databaseFile.Close()

	switch command {
	case ".dbinfo":
		header := make([]byte, 100)
		if _, err := databaseFile.Read(header); err != nil {
			log.Fatal(err)
		}

		var pageSize uint16
		if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
			fmt.Println("Failed to read page size:", err)
			return
		}

		// 2. Read Page Header (first page starts at 100 for SQLite)
		// Byte 3 and 4 contains the number of cells (tables in sqlite_schema)
		pageHeader := make([]byte, 8)
		if _, err := databaseFile.Read(pageHeader); err != nil {
			log.Fatal(err)
		}

		var numberOfTables uint16
		if err := binary.Read(bytes.NewReader(pageHeader[3:5]), binary.BigEndian, &numberOfTables); err != nil {
			fmt.Println("Failed to read cell count:", err)
			return
		}

		fmt.Printf("database page size: %v\n", pageSize)
		fmt.Printf("number of tables: %v\n", numberOfTables)

	case ".tables":
		// Skip the 100-byte database header
		databaseFile.Seek(100, 0)

		pageHeader := make([]byte, 8)
		if _, err := databaseFile.Read(pageHeader); err != nil {
			log.Fatal(err)
		}

		var numberOfTables uint16
		binary.Read(bytes.NewReader(pageHeader[3:5]), binary.BigEndian, &numberOfTables)

		// Read cell pointers
		cellPointerArrBuffer := make([]byte, numberOfTables*2)
		if _, err := databaseFile.Read(cellPointerArrBuffer); err != nil {
			log.Fatal(err)
		}

		var cellPointerArr []uint16
		for i := 0; i < int(numberOfTables); i++ {
			var val uint16
			binary.Read(bytes.NewReader(cellPointerArrBuffer[i*2:(i*2)+2]), binary.BigEndian, &val)
			cellPointerArr = append(cellPointerArr, val)
		}

		// Parse each cell to get the table name
		for _, cell := range cellPointerArr {
			databaseFile.Seek(int64(cell), 0)
			cellReader := bufio.NewReader(databaseFile)

			binary.ReadUvarint(cellReader) // payload size
			binary.ReadUvarint(cellReader) // row id

			recordHeaderSize, _ := binary.ReadUvarint(cellReader)

			types := []uint64{}
			bytesRead := uint64(getVarintSize(recordHeaderSize))

			// recordHeaderSize includes the size of the varint itself in your logic
			// Adjusted to match the byte-tracking logic from original code
			bytesRead = 1
			for bytesRead < recordHeaderSize {
				t, _ := binary.ReadUvarint(cellReader)
				types = append(types, t)
				bytesRead += uint64(getVarintSize(t))
			}

			// Serial Type 13+ (odd) means a string of length (N-13)/2
			// Col 0: type, Col 1: name, Col 2: tbl_name
			col1Size := (types[0] - 13) / 2
			col2Size := (types[1] - 13) / 2
			tableNameSize := (types[2] - 13) / 2

			cellReader.Discard(int(col1Size + col2Size))

			nameBuf := make([]byte, tableNameSize)
			cellReader.Read(nameBuf)

			fmt.Printf("%s ", string(nameBuf))
		}
		fmt.Println() // New line after listing all tables

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

func getVarintSize(v uint64) int {
	if v <= 127 {
		return 1
	}
	if v <= 16383 {
		return 2
	}
	if v <= 2097151 {
		return 3
	}
	if v <= 268435455 {
		return 4
	}
	if v <= 34359738367 {
		return 5
	}
	if v <= 4398046511103 {
		return 6
	}
	if v <= 562949953421311 {
		return 7
	}
	if v <= 72057594037927935 {
		return 8
	}
	return 9
}
