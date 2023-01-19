# mass-crc32c
Computes Google GCS compatible CRC32C of local files with configurable multithreading and parallel file reads 

The initial use-case was to compute an inventory with CRC32C of a billion files on a local storage prior to upload to GCS.

# usage
```
$ mass-crc32c --help
Usage of ./mass-crc32c: [options] path [path ...]

Options:
  -j int
    	# of parallel reads (default 1)
  -l int
    	size of list ahead queue (default 100)
  -p int
    	# of cpu used (default 1)
  -s int
    	size of reads in kbytes (default 1)
```

# Release
This project uses [goreleaser](https://goreleaser.com/)
You can follow this [quick start guide](https://goreleaser.com/quick-start/) to create a new release
