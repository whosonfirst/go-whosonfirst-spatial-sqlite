package tests

import (
	"context"
	"github.com/whosonfirst/go-reader"
	"io"
	"io/ioutil"
	"testing"
)

func TestNullReader(t *testing.T) {

	ctx := context.Background()

	r, err := reader.NewReader(ctx, "null:/")

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
