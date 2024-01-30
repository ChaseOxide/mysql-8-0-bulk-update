package main

import (
	"database/sql"
	"fmt"
	"github.com/ChaseOxide/mysql-8-0-bulk-update/bootstrap"
	"github.com/ChaseOxide/mysql-8-0-bulk-update/inserter"
	"os"
	"slices"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type runData struct {
	Method   string
	Duration time.Duration
	Count    int
}

func main() {
	var outputFilename string
	var byId bool
	var noAtom bool
	var multiplier int
	var iter int

	multiplier = 100
	iter = 10
	helpOutput := "mysql-8-0-bulk-update [--id] [--no-atom] [--multiplier MULTIPLIER] [--iter ITER] outputFilename"

	for i := 1; i < len(os.Args); i += 1 {
		var err error

		arg := os.Args[i]
		switch arg {
		case "--id":
			byId = true
		case "--no-atom":
			noAtom = true
		case "--multiplier":
			if i+1 == len(os.Args) {
				printAndExit(helpOutput)
			}
			i += 1
			multiplier, err = strconv.Atoi(os.Args[i])
			if err != nil {
				printAndExit(helpOutput)
			}
			if multiplier < 1 {
				fmt.Println("multiplier value must be more than 0")
				os.Exit(1)
			}
		case "--iter":
			if i+1 == len(os.Args) {
				printAndExit(helpOutput)
			}
			i += 1
			iter, err = strconv.Atoi(os.Args[i])
			if err != nil {
				printAndExit(helpOutput)
			}
		case "--help":
			fmt.Println(helpOutput)
			return
		default:
			if outputFilename != "" {
				printAndExit(helpOutput)
			}
			outputFilename = arg
		}
	}
	if outputFilename == "" {
		printAndExit(helpOutput)
	}

	db, err := sql.Open("mysql", "root:@/db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	tableName := "people"

	runFns := []func() runData{}

	for i := 1; i <= iter; i += 1 {
		var updateInstrs []inserter.UpdateInstr

		count := multiplier * i
		if byId {
			updateInstrs = inserter.GenerateUpdateInstrWithId(count, 0.3)
		} else {
			updateInstrs = inserter.GenerateUpdateInstr(count, 0.3)
		}

		runFns = append(runFns, func() runData {
			start := time.Now()
			err := inserter.JoinUpdate(db, tableName, updateInstrs)

			if err != nil {
				return runData{
					"join",
					-1,
					count,
				}
			}

			return runData{
				"join",
				time.Since(start),
				count,
			}
		}, func() runData {
			start := time.Now()
			err := inserter.CaseUpdate(db, tableName, updateInstrs)

			if err != nil {
				return runData{
					"case",
					-1,
					count,
				}
			}

			return runData{
				"case",
				time.Since(start),
				count,
			}
		})

		if !noAtom {
			runFns = append(runFns, func() runData {
				start := time.Now()
				tx, err := db.Begin()
				if err != nil {
					panic(err)
				}
				err = inserter.AtomUpdate(tx, tableName, updateInstrs)
				if err != nil {
					return runData{
						"atom",
						-1,
						count,
					}
				}
				err = tx.Commit()
				if err != nil {
					panic(err)
				}
				return runData{
					"atom",
					time.Since(start),
					count,
				}
			})
		}
	}

	f, err := os.Create(outputFilename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	insertQuery := bootstrap.GenerateInsertQuery(tableName, 100000, 0.5)

	countSet := map[int]bool{}
	collectedData := map[string]map[int]runData{}
	for _, fn := range runFns {
		bootstrap.RecreateTable(db, tableName)
		db.Exec(insertQuery)

		data := fn()
		fmt.Printf("%s: %v [%d]\n", data.Method, data.Duration, data.Count)

		countSet[data.Count] = true

		if _, ok := collectedData[data.Method]; !ok {
			collectedData[data.Method] = map[int]runData{}
		}
		collectedData[data.Method][data.Count] = data
	}

	counts := make([]int, 0, len(countSet))
	for count := range countSet {
		counts = append(counts, count)
	}
	slices.Sort(counts)

	for _, count := range counts {
		fmt.Fprintf(f, ",%d", count)
	}
	f.WriteString("\n")
	for name, data := range collectedData {
		f.WriteString(name)
		for _, count := range counts {
      if data[count].Duration < 0 {
        f.WriteString(",")
      } else {
        fmt.Fprintf(f, ",%d", data[count].Duration.Milliseconds())
      }
		}
		f.WriteString("\n")
	}
}

func printAndExit(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
