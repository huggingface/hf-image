//go:build linux

package main

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

type entry struct {
	name, link string
	kind       byte
	data       string
}

func layer(t *testing.T, entries ...entry) []byte {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	for _, e := range entries {
		h := &tar.Header{Name: e.name, Linkname: e.link, Typeflag: e.kind, Mode: 0o644, Size: int64(len(e.data))}
		if e.kind == 0 {
			h.Typeflag = tar.TypeReg
		}
		if h.Typeflag != tar.TypeReg {
			h.Size = 0
		}
		if err := w.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(e.data)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func needsRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root: whiteouts are device nodes, opaque markers trusted xattrs")
	}
}

// snapshot describes dir: its entries, their modes and contents, and every xattr.
func snapshot(t *testing.T, dir string) string {
	t.Helper()
	var out bytes.Buffer
	err := filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		out.WriteString(p + " " + fi.Mode().String())
		if fi.Mode().IsRegular() {
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			out.Write(data)
		}
		buf := make([]byte, 4096)
		n, _ := unix.Llistxattr(p, buf)
		out.Write(buf[:max(n, 0)])
		out.WriteString("\n")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func TestLayersStayInTheirDirectory(t *testing.T) {
	needsRoot(t)
	sym, reg, link := byte(tar.TypeSymlink), byte(tar.TypeReg), byte(tar.TypeLink)
	for name, entries := range map[string]func(victim string) []entry{
		"whiteout through a symlink": func(v string) []entry {
			return []entry{{name: "a", kind: sym, link: v}, {name: "a/.wh.secret"}}
		},
		"opaque marker through a symlink": func(v string) []entry {
			return []entry{{name: "a", kind: sym, link: v}, {name: "a/.wh..wh..opq"}}
		},
		"file through a symlink": func(v string) []entry {
			return []entry{{name: "a", kind: sym, link: v}, {name: "a/pwned", kind: reg, data: "x"}}
		},
		"file through a relative symlink": func(v string) []entry {
			return []entry{{name: "a", kind: sym, link: "../../../../../../.." + v}, {name: "a/pwned", kind: reg, data: "x"}}
		},
		"directories through a symlink": func(v string) []entry {
			return []entry{{name: "a", kind: sym, link: v}, {name: "a/b/c", kind: reg, data: "x"}}
		},
		"hard link out": func(v string) []entry {
			return []entry{{name: "a", kind: link, link: v + "/secret"}}
		},
		"dot-dot": func(v string) []entry {
			return []entry{{name: "../../../../../../.." + v + "/pwned", kind: reg, data: "x"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			victim, dir := t.TempDir(), t.TempDir()
			if err := os.WriteFile(filepath.Join(victim, "secret"), []byte("secret"), 0o644); err != nil {
				t.Fatal(err)
			}
			before := snapshot(t, victim)
			_ = apply(dir, bytes.NewReader(layer(t, entries(victim)...)))
			if after := snapshot(t, victim); after != before {
				t.Fatalf("the layer changed %s:\n%s\nnow:\n%s", victim, before, after)
			}
		})
	}
}

func TestWhiteoutsBecomeOverlayfsMarkers(t *testing.T) {
	needsRoot(t)
	dir := t.TempDir()
	tarball := layer(t,
		entry{name: "etc", kind: tar.TypeDir},
		entry{name: "etc/hello", data: "hello"},
		entry{name: "etc/.wh.gone"},
		entry{name: "opq", kind: tar.TypeDir},
		entry{name: "opq/.wh..wh..opq"},
	)
	stdin := bytes.NewReader(append(tarball, make([]byte, 10240)...))
	if err := apply(dir, stdin); err != nil {
		t.Fatal(err)
	}
	if stdin.Len() != 0 {
		t.Fatalf("%d bytes left unread", stdin.Len())
	}
	if data, err := os.ReadFile(filepath.Join(dir, "etc/hello")); err != nil || string(data) != "hello" {
		t.Fatalf("etc/hello: %q, %v", data, err)
	}
	var st unix.Stat_t
	if err := unix.Lstat(filepath.Join(dir, "etc/gone"), &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFCHR || st.Rdev != 0 {
		t.Fatalf("etc/gone is not a 0/0 character device: %+v, %v", st, err)
	}
	buf := make([]byte, 8)
	if n, err := unix.Lgetxattr(filepath.Join(dir, "opq"), "trusted.overlay.opaque", buf); err != nil || string(buf[:n]) != "y" {
		t.Fatalf("opq is not opaque: %q, %v", buf[:max(n, 0)], err)
	}
}
