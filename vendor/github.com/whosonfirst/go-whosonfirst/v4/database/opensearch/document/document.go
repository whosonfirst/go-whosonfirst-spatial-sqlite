// Package document provides methods for updating a single Who's On First document for indexing in OpenSearch.
//
// Note: One of the things you'll see in the code that makes up the `document` package is stuff like this:
//
//	for k, v := range to_assign {
//
//		path := k
//
//		if props_rsp.Exists() {
//			path = fmt.Sprintf("properties.%s", k)
//		}
//
//		body, err = sjson.SetBytes(body, path, v)
//		...
//	}
//
// This is code to account for the fact that a record may be a "spelunker v1" document in which case it will
// be a simple hash map, equivalent to a GeoJSON properties dictionary, rather than a complete GeoJSON document.
package document

import (
	"context"
)

// type PrepareDocumentFunc is a common method signature updating a Who's On First document for indexing in OpenSearch.
type PrepareDocumentFunc func(context.Context, []byte) ([]byte, error)
