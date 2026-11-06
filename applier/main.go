//go:build linux

// hf-image-apply unpacks a layer tar, read from stdin, into an overlayfs upper directory with
// containerd's own applier: the options its overlayfs differ uses, without lower directories as in
// its parallel unpack.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/containerd/containerd/v2/pkg/archive"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: hf-image-apply <dir> < layer.tar")
		os.Exit(2)
	}
	if err := apply(os.Args[1], os.Stdin); err != nil {
		fmt.Fprintf(os.Stderr, "hf-image-apply: %v\n", err)
		os.Exit(1)
	}
}

func apply(dir string, stdin io.Reader) error {
	r := bufio.NewReaderSize(stdin, 1<<20)
	if _, err := archive.Apply(context.Background(), dir, r, archive.WithConvertWhiteout(archive.OverlayConvertWhiteout)); err != nil {
		return err
	}
	// The padding after the end of the archive: the caller hashes every byte it sends.
	_, err := io.Copy(io.Discard, r)
	return err
}
