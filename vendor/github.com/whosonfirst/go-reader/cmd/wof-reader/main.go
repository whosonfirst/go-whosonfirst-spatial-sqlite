package main

import (
	"context"
	"flag"
	"github.com/whosonfirst/go-reader"
	"io"
	"log"
	"os"
)

func main() {

	path := flag.String("path", "", "")

	source := flag.String("source", "", "")

	flag.Parse()

	ctx := context.Background()

	r, err := reader.NewReader(ctx, *source)

	if err != nil {
		log.Fatal(err)
	}

	fh, err := r.Read(ctx, *path)

	if err != nil {
		log.Fatal(err)
	}

	defer fh.Close()

	io.Copy(os.Stdout, fh)
}
