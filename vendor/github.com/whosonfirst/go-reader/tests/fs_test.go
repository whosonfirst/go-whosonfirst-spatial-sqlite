package tests

import (
	"context"
	"fmt"
	"github.com/whosonfirst/go-reader"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestFSReader(t *testing.T) {

	ctx := context.Background()

	cwd, err := os.Getwd()

	if err != nil {
		t.Fatal(err)
	}

	source := fmt.Sprintf("fs://%s/fixtures", cwd)
	r, err := reader.NewReader(ctx, source)

	if err != nil {
		t.Fatal(err)
	}

	fh, err := r.Read(ctx, "101/736/545/101736545.geojson")

	if err != nil {
		t.Fatal(err)
	}

	defer fh.Close()

	_, err = io.Copy(ioutil.Discard, fh)

	if err != nil {
		t.Fatal(err)
	}
}
