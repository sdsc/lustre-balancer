package main

import (
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"time"
	"log"
	"os/exec"
	"encoding/csv"
	"strings"
)

var (
	app = kingpin.New("lustre-balancer", "Lustre balancer tool.")
	filesystemParam = app.Arg("mountpoint", "Lustre mount point.").Required().ExistingDir()
)

type ost struct {
	index int
	free int64
}

var osts []ost

func getOsts(num int, fromTop bool) (res []int, err error) {
	var resOst []int
	return resOst, nil
}

func readOsts(){
    for range time.Tick(time.Second * 60){

		out, err := exec.Command("lfs", "df", "-l").Output()
		if err != nil {
			log.Fatal(err)
		}

		r := csv.NewReader(strings.NewReader(string(out)))
		r.Comma = ' '
		_, err = r.ReadAll()
		if err != nil {
			log.Fatal(err)
		}
    }
}


func main() {
	kingpin.MustParse(app.Parse(os.Args[1:]))

    go readOsts()

	//filesChan := make(chan string)


}