package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

func runToken(args []string) int {
	flags := flag.NewFlagSet("token", flag.ContinueOnError)
	user := flags.String("user", "", "user id the token is for (required)")
	auth := registerAuthFlags(flags)
	err := flags.Parse(args)
	if err != nil {
		return 2
	}
	if *user == "" {
		fmt.Fprintln(os.Stderr, "token: -user is required")
		return 2
	}

	minter, err := auth.minter()
	if err != nil {
		fmt.Fprintf(os.Stderr, "token: %v\n", err)
		return 1
	}
	value, err := minter.Mint(*user, time.Now())
	if err != nil {
		fmt.Fprintf(os.Stderr, "mint token: %v\n", err)
		return 1
	}
	fmt.Println(value)
	return 0
}
