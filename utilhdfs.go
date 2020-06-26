package main

import (
	"fmt"
	"github.com/colinmarc/hdfs"
	"github.com/scritchley/orc"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type Settings struct {
	Cluster struct {
		Address []string `yaml:",flow"`
	}
	Data struct {
		Path []string `yaml:",flow"`
	}
}

type DataPack struct {
	p      hdfs.FileReader
	offset int64
	size   int64
	fInfo  os.FileInfo
	client *hdfs.Client
}

func (t DataPack) Size() int64 {
	return t.fInfo.Size()
}

func (t DataPack) ReadAt(p []byte, off int64) (n int, err error) {
	fileName := PathHdfs + t.fInfo.Name()
	file, err := t.client.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	n, err = file.ReadAt(p, off)
	return n, err
}

var PathHdfs string = ""

func NewReadAtUn(client *hdfs.Client, offset int64, fileInfo os.FileInfo) orc.SizedReaderAt {
	return DataPack{client: client, offset: offset, fInfo: fileInfo}
}

const fileName = "conf.yaml"

func main() {

	yamlFile, err := ioutil.ReadFile(fileName)
	if !(err == nil) {
		panic(err)
	}

	cFile := Settings{}
	err = yaml.Unmarshal([]byte(yamlFile), &cFile)
	if !(err == nil) {
		panic(err)
	}

	fmt.Printf("Result: %v\n", cFile)

	PathHdfs = cFile.Data.Path[0]

	fmt.Print("HDFS Read...")

	// Create new client to HDFS
	client, _ := hdfs.New(cFile.Cluster.Address[0] /*"localhost:8020"*/)

	// Red HDFS directory
	fInfos, err := client.ReadDir(PathHdfs)
	if !(err == nil) {
		panic(err)
	}

	// Iterate files from directory
	for _, finFo := range fInfos {

		// read only files
		if finFo.IsDir() != true && strings.Contains(finFo.Name(), ".orc") {

			// Print name and size of the file.
			fmt.Println(finFo.Name(), finFo.Size())

			data := NewReadAtUn(client, 0, finFo)

			orcOps, _ := orc.NewReader(data)

			orcOps.Schema().Columns()

			c := orcOps.Select(orcOps.Schema().Columns()...) //"col_1" , "col_2", "col_3")

			for c.Stripes() {
				for c.Next() {
					log.Println(c.Row())
				}
			}
			if err := c.Err(); err != nil {
				log.Fatal(err)
			}

		}
	}
}
