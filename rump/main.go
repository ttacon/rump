package main

import (
	"flag"
	"fmt"

	"github.com/ttacon/rump"
)

var (
	dumpFile = flag.String("dmp", "", "location of the dump file to inspect")
	key      = flag.String("k", "", "the key to inspect")

	// actions we can take
	keyType = flag.Bool("type", false, "print the type of the desired key")
	value   = flag.Bool("value", false, "print the value of the desired key")
)

func main() {
	flag.Parse()

	if len(*dumpFile) == 0 {
		// for the moment, if we don't have a dump file to inspect
		// print usage and quit
		flag.Usage()
		return
	}

	doit, err := desiredFunc()
	if err != nil {
		fmt.Println(err)
		return
	}

	err = doit()
	if err != nil {
		fmt.Println(err)
	}
}

func desiredFunc() (func() error, error) {
	// if no mode, just print info about file and exit
	r, err := rump.New(*dumpFile, *key)
	if err != nil {
		return nil, err
	}

	var f = func() error { return nil }

	if *keyType {
		oldF := f
		f = func() error {
			err := oldF()
			if err != nil {
				return err
			}

			keyType, err := r.KeyType()
			if err != nil {
				return err
			}
			fmt.Println(keyType)
			return nil
		}
	}

	if *value {
		oldF := f
		f = func() error {
			err := oldF()
			if err != nil {
				return err
			}

			r.Value()
			return nil
		}
	}

	return f, nil
}
