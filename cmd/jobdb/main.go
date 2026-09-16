package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/colony-2/jobdb/pkg/jobdb/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := cli.NewRootCmd().ExecuteContext(ctx); err != nil {
		log.Fatal(err)
	}
}
