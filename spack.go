// Copyright (c) 2016 Dominik Zeromski <dzeromsk@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package main

import (
	"archive/tar"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/klauspost/compress/gzip"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Invalid args!")
	}

	var test []string
	filepath.Walk(os.Args[2], func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		test = append(test, path)
		return nil
	})

	log.Fatal(Archive(context.Background(), test))
}

type result struct {
	path   string
	sum    [md5.Size]byte
	offset int64
	size   int64
}

func Archive(ctx context.Context, filelist []string) error {
	g, ctx := errgroup.WithContext(ctx)

	files := make(chan result)
	g.Go(func() error {
		defer close(files)
		for _, f := range filelist {
			var t result
			t.path = f
			select {
			case files <- t:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	logs := make(chan result)
	g.Go(func() error {
		defer close(logs)
		group := ArchiveGroup(ctx, files, logs)
		return group.Wait()
	})

	for _ = range logs {
		//	fmt.Printf("%s %x %d\n", log.path, log.sum, log.size)
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func ArchiveGroup(ctx context.Context, input, output chan result) *errgroup.Group {
	g, ctx := errgroup.WithContext(ctx)

	md5s := make(chan result)
	g.Go(func() error {
		defer close(md5s)
		group := MD5Group(ctx, input, md5s)
		return group.Wait()
	})

	archives := make(chan result)
	g.Go(func() error {
		defer close(archives)
		group := CompressGroup(ctx, md5s, archives)
		return group.Wait()
	})

	g.Go(func() error {
		group := WriteGroup(ctx, archives, output)
		return group.Wait()
	})

	return g
}

func MD5Group(ctx context.Context, input, output chan result) *errgroup.Group {
	g, ctx := errgroup.WithContext(ctx)

	const n = 8
	for i := 0; i < n; i++ {
		g.Go(func() error {
			for i := range input {
				data, err := ioutil.ReadFile(i.path)
				if err != nil {
					return err
				}
				select {
				case output <- result{i.path, md5.Sum(data), 0, 0}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}

	return g
}

func CompressGroup(ctx context.Context, input, output chan result) *errgroup.Group {
	g, ctx := errgroup.WithContext(ctx)

	const n = 8
	for i := 0; i < n; i++ {
		g.Go(func() error {
			for i := range input {
				path := fmt.Sprintf("%s/spack%x", os.TempDir(), i.sum)

				info, err := os.Stat(path)
				if err != nil {
					tmpfile, err := CompressFile(i.path)
					if err != nil {
						return err
					}

					err = os.Rename(tmpfile, path)
					if err != nil {
						return err
					}

					info, err = os.Stat(path)
					if err != nil {
						return err
					}
				}

				select {
				case output <- result{path, i.sum, 0, info.Size()}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}

	return g
}

func WriteGroup(ctx context.Context, input, output chan result) *errgroup.Group {
	g, ctx := errgroup.WithContext(ctx)

	chunks := make(chan result)
	g.Go(func() error {
		defer close(chunks)
		var offset int64
		for i := range input {
			select {
			case chunks <- result{i.path, i.sum, offset, i.size}:
				offset += i.size
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil

	})

	const n = 8
	for i := 0; i < n; i++ {
		g.Go(func() error {
			for i := range chunks {
				err := InsertFile(os.Args[1], i.path, i.offset)
				if err != nil {
					return err
				}
				select {
				case output <- i:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}

	return g
}

func InsertFile(dst, src string, offset int64) error {
	d, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE, 0664)
	if err != nil {
		return err
	}
	defer d.Close()

	s, err := os.Open(src)
	if err != nil {
		return err
	}
	defer s.Close()

	_, err = d.Seek(offset, 0)
	if err != nil {
		return err
	}

	_, err = io.Copy(d, s)
	if err != nil {
		return err
	}

	return nil
}

func CompressFile(src string) (dst string, err error) {
	s, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer s.Close()

	sinfo, err := s.Stat()
	if err != nil {
		return "", err
	}

	d, err := ioutil.TempFile("", "spack")
	if err != nil {
		return "", err
	}
	defer d.Close()

	z := gzip.NewWriter(d)

	t := tar.NewWriter(z)
	// do not close to avoid EOF marker

	header := &tar.Header{
		Name:    src,
		Mode:    int64(sinfo.Mode()),
		Size:    sinfo.Size(),
		ModTime: sinfo.ModTime(),
	}

	err = t.WriteHeader(header)
	if err != nil {
		return "", err
	}

	_, err = io.Copy(t, s)
	if err != nil {
		return "", err
	}

	err = t.Flush()
	if err != nil {
		return "", err
	}

	err = z.Close()
	if err != nil {
		return "", err
	}

	return d.Name(), nil
}
