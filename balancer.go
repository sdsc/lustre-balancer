package main

import (
	"bufio"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
	"fmt"
	"github.com/dustin/go-humanize"
)

const LustreNoStripeSize = 10 * 1000000000
const Lustre5StripeSize = 100 * 1000000000
const Lustre10StripeSize = 1000 * 1000000000

var topOstsList []string

var filesMigrated, bytesMigrated uint64

var (

	filesystemParam = kingpin.Arg("mountpoint", "Lustre folder to look in.").Required().ExistingDir()
	searchOstsNumParam = kingpin.Flag("ostsearch", "Number of most used OSTs to search for.").Short('o').Default("5").Int()
	searchFileSizeParam = kingpin.Flag("size", "Minimal file size to search for.").Short('s').Default("+1M").String()
)

type Ost struct {
	Index int
	Free  int64
}


func (ostObj Ost) String() string {
	return fmt.Sprintf("%d", ostObj.Index)
}


type Osts []Ost

func (slice Osts) Len() int {
	return len(slice)
}

func (slice Osts) Less(i, j int) bool {
	return slice[i].Free > slice[j].Free
}

func (slice Osts) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

var curOsts Osts

func getOsts(num int, top bool) []Ost {
	if top {
		return curOsts[0:num]
	}
	return curOsts[len(curOsts)-num:]
}

func readOsts() {
	var newOsts Osts
	out, err := exec.Command("lfs", "df", "-l", *filesystemParam).Output()
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		line := strings.Fields(scanner.Text())
		if len(line) == 6 {
			if free, err := strconv.ParseInt(line[3], 10, 64); err == nil {
				ostPos := strings.Index(line[5], "OST:")
				if ostPos > -1 {
					if ostInd, err := strconv.Atoi(line[5][ostPos+4 : strings.Index(line[5], "]")]); err == nil {
						newOsts = append(newOsts, Ost{ostInd, free})
					}
				}
			}
		}
	}

	sort.Sort(newOsts)
	curOsts = newOsts
}

func printStatus() {
	fmt.Printf("\rSearching OSTs: %v, migrated files: %d, bytes: %s", topOstsList, filesMigrated, humanize.Bytes(bytesMigrated))
}


func main() {

	kingpin.New("lustre-balancer", "Lustre balancer tool.")
	kingpin.Version("1.0").Author("Dmitry Mishin <dmishin@sdsc.edu>")
	kingpin.CommandLine.Help = "The tool rebalances lustre OSTs by searching the files on OST with lowest free space and migrating them to OST with highest free space."
	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()


	readOsts()

	go func() {
		for range time.Tick(time.Second * 60) {
			readOsts()
		}
	}()

	go func() {
		for range time.Tick(time.Second * 2) {
			printStatus()
		}
	}()

	searchOstsNum := *searchOstsNumParam

	for _, curOst := range getOsts(searchOstsNum, false) {
		topOstsList = append(topOstsList, fmt.Sprintf("%d", curOst.Index))
	}

	cmdName := "lfs"
	cmdArgs := []string{"find", *filesystemParam, "--type", "f", "--size", *searchFileSizeParam, "--ost", strings.Join(topOstsList, ",")}

	cmd := exec.Command(cmdName, cmdArgs...)
	cmdReader, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("Error creating StdoutPipe for Cmd", err)
		os.Exit(1)
	}

	filesChan := make(chan string, 1048576)
	scanner := bufio.NewScanner(cmdReader)
	go func() {
		defer close(filesChan)
		for scanner.Scan() {
			filesChan <- scanner.Text()
		}
	}()

	go func() {
		for newFile := range filesChan {
			fi, e := os.Stat(newFile);
			if e == nil {
				size := fi.Size()

				toOstsNum := 1
				if size > LustreNoStripeSize {
					if size < Lustre5StripeSize {
						toOstsNum = 5
					} else if size < Lustre10StripeSize {
						toOstsNum = 10
					} else {
						toOstsNum = 50
					}
				}

				var toOstsList []string
				for _, curOst := range getOsts(toOstsNum, true) {
					toOstsList = append(toOstsList, fmt.Sprintf("%d", curOst.Index))
				}

				//log.Printf("Migrating file %s of size %d to OSTs %v", newFile, size, toOstsList)
				err := exec.Command("lfs", "migrate", "-o", strings.Join(toOstsList, ","), newFile).Run()
				if err != nil {
					log.Printf("Error doing migrate of file %s: %s", newFile, err)
				}
				filesMigrated += 1
				bytesMigrated += uint64(size)
			}
		}
	}()

	err = cmd.Start()
	if err != nil {
		log.Fatalf("Error starting Cmd", err)
		os.Exit(1)
	}

	err = cmd.Wait()
	if err != nil {
		log.Fatalf("Error waiting for Cmd", err)
		os.Exit(1)
	}

}
