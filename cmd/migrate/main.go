package main

import (
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"os"
)

func main() {
	if len(os.Args) != 2 || (os.Args[1] != "up" && os.Args[1] != "down") {
		fmt.Fprintln(os.Stderr, "usage: migrate up|down")
		os.Exit(2)
	}
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_URL required")
		os.Exit(2)
	}
	m, err := migrate.New("file://migrations", url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "migration initialization failed")
		os.Exit(1)
	}
	if os.Args[1] == "up" {
		err = m.Up()
	} else {
		err = m.Down()
	}
	if err != nil && err != migrate.ErrNoChange {
		fmt.Fprintln(os.Stderr, "migration failed")
		os.Exit(1)
	}
}
