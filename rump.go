package rump

import (
	"errors"
	"fmt"
	"os"
)

type Rump struct {
	f          *os.File
	desiredKey string
	todos      []func()
	curr       int64
}

func New(filename, desiredKey string) (*Rump, error) {
	// ensure the file exists
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	// let's double check it's a redis file
	buf := make([]byte, 9)
	_, err = f.Read(buf)
	if err != nil {
		return nil, err
	}
	if string(buf[0:5]) != "REDIS" {
		return nil, errors.New("file is not a redis dump file")
	}

	return &Rump{
		f:          f,
		desiredKey: desiredKey,
	}, nil
}

func (r *Rump) Close() error {
	return r.f.Close()
}

func (r *Rump) SetTodos(fs ...func()) {
	r.todos = append(r.todos, fs...)
}

func (r *Rump) KeyType() (string, error) {
	return r.findKey()
}

func (r *Rump) Value() (string, error) {
	kType, err := r.KeyType()
	if err != nil {
		return "", err
	}

	return r.readVal(kType)
}

// TODO(ttacon): extract key types out to constants
func (r *Rump) readVal(kType string) (string, error) {
	switch kType {
	case "string":
		return r.readStringValue()
	}

	return "", nil
}

func (r *Rump) readStringValue() (string, error) {
	buf := make([]byte, 4096)
	n, err := r.f.Read(buf)
	if err != nil {
		fmt.Println("n: ", n)
		return "", err
	}

	size := 0

	lengthEncoding := buf[0]
	if lengthEncoding&0xc0 == 0 {
		size = int(lengthEncoding)
		return string(buf[1 : size+1]), nil
	}

	// deal with compressed string
	if lengthEncoding&0xc0 == 0xc0 {
		fmt.Println("it's encoded in a special format")
		format := lengthEncoding & 0x3f
		fmt.Println("format: ", format)
		if format == 4 || format == 3 {
			// compressed string
			cLen := buf[1]
			var compressedSize int64 = 0
			fmt.Println("cLen: ", cLen)
			if cLen&0xc0 == 0 {
				compressedSize = int64(cLen)
			} else {
				return "", errors.New("unsuported 2")
			}

			fmt.Println("compressedSize: ", compressedSize)
			origLen := buf[2]
			fmt.Println("origLen: ", origLen)
			fmt.Printf("%x\n", origLen&0xc0)
			var originalSize int64 = 0
			if origLen&0xc0 == 0 {
				fmt.Println("use 6 bits")
				originalSize = int64(origLen)
			} else if origLen&0xc0 == 0x40 {
				fmt.Printf("use 4 bytes: %x\n", buf[3])
				originalSize = int64(buf[3])
			}

			fmt.Println("originalSize: ", originalSize)
			compressedData := buf[4 : 4+compressedSize]
			fmt.Printf("compressedData: % x\n", compressedData)
			decomp, err := decompressLZF(compressedSize, originalSize, compressedData)
			fmt.Println("err: ", err)
			fmt.Println("decomp: ", string(decomp))

		}
	}
	fmt.Printf("length: %x\n", lengthEncoding&0xc0)
	return "", errors.New("currently not supported")
}

func decompressLZF(cSize, oSize int64, data []byte) ([]byte, error) {
	var cursor, outCursor int64
	var curr byte = 0
	var out []byte
	// this is translated from lzf_d.c
	var dataLen = int64(len(data))
	for cursor < dataLen {

		curr = data[cursor]
		cursor++

		if curr < 32 { /*  literal run */
			curr++

			out = append(out, data[cursor:cursor+int64(curr)]...)
			outCursor += int64(curr)
			cursor += int64(curr)
		} else {
			length := curr >> 5
			if length == 7 {
				length += curr
				cursor++
			}

			ref := outCursor - int64(((curr & 0x1f) << 8)) - int64(data[cursor]) - 1
			cursor++

			fmt.Println("len(out): ", len(out))
			fmt.Println("out: ", out)
			fmt.Println("ref: ", ref)
			fmt.Println("length: ", length)
			fmt.Println("curr: ", curr)
			fmt.Println("curr >> 5: ", curr>>5)
			fmt.Println("data[cursor]: ", data[cursor])
			for i := ref; i < ref+int64(length)+2; i++ {
				fmt.Printf("i: %d, len(out): %d\n", i, len(out))
				out = append(out, out[i])
			}
		}
	}
	return out, nil
}

func (r *Rump) Reset() error {
	off, err := r.f.Seek(9, 0)
	if err != nil {
		return err
	}
	if off != 9 {
		return errors.New("couldn't reset file appropriately")
	}
	return nil
}

// if error is non-nil, the file descriptor is quaranteed to be sitting in
// front of it in the file
func (r *Rump) findKey() (string, error) {
	// unfortunately, to find the key, we're just gonna have to do a linear search...
	// here's to hoping we get lucky
	// TODO(ttacon): divide dumpfile into sectors and run go routines over

	// TODO(ttacon): decide on a good sized byte slice to use
	var (
		buf          = make([]byte, 4096)
		stillLooking = true
	)

	for stillLooking {
		n, err := r.f.Read(buf)
		if err != nil {
			fmt.Println("n: ", n)
			return "", err
		}
		cursor := 0
		// make sure we're not at the boundary of a db
		if buf[cursor] == 0xfe {
			cursor += 2
		}

		// see if the key has a ttl
		if buf[cursor] == 0xfd {
			cursor += 9 // TODO(ttacon): not sure about this?
		} else if buf[cursor] == 0xfc {
			cursor += 9
		}

		// see what type it is
		switch buf[cursor] {
		case 0x00:
			cursor += 1
			found, extraOffset, err := r.readStringKey(cursor, buf)
			if err != nil {
				return "", err
			}
			cursor += extraOffset

			if r.desiredKey == found {
				v, err := r.f.Seek(int64(cursor-n), 1)
				if err != nil {
					fmt.Println("v: ", v)
					return "", err
				}
				return "string", nil
			}
		default:
			fmt.Println("not a string. it's not a string")
			return "", nil
		}
	}

	return "", nil
}

func (r *Rump) readStringKey(cursor int, buf []byte) (string, int, error) {
	lengthEncoding := buf[cursor] & 0xc0
	var size, offset int

	switch lengthEncoding {
	case 0x00:
		offset = 1
		size = int(buf[cursor])
	case 0x01:
		// TODO(ttacon): do it
		return "", 0, errors.New("not handled 1")
	case 0x02:
		// TODO(ttacon): do it
		return "", 0, errors.New("not handled 2")
	case 0x03:
		// TODO(ttacon): do it
		return "", 0, errors.New("not handled 3")
	}

	// TODO(ttacon): make sure we have room left in buf for this
	return string(buf[cursor+offset : cursor+offset+size]), offset + size, nil
}

/*
func (f *File) Read(b []byte) (n int, err error)
    Read reads up to len(b) bytes from the File. It returns the number of
    bytes read and an error, if any. EOF is signaled by a zero count with
    err set to io.EOF.

func (f *File) Seek(offset int64, whence int) (ret int64, err error)
    Seek sets the offset for the next Read or Write on file to offset,
    interpreted according to whence: 0 means relative to the origin of the
    file, 1 means relative to the current offset, and 2 means relative to
    the end. It returns the new offset and an error, if any.
*/
