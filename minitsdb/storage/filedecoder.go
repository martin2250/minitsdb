package storage

import (
	"github.com/martin2250/minitsdb/minitsdb/storage/encoding"
	"io"
)

// FileDecoder decodes blocks from multiple files and handles opening / closing files
type FileDecoder struct {
	files       []*DataFile // files to be read
	decoder     encoding.Decoder
	currentFile *DataFileReader // file that is currently being read (only held for closing)
}

func (d *FileDecoder) nextFile() error {
	if len(d.files) == 0 {
		return io.EOF
	}

	d.Close()

	var err error
	d.currentFile, err = d.files[0].GetReader()

	if err != nil {
		return err
	}

	d.decoder.SetReader(d.currentFile)
	d.files = d.files[1:]

	return nil
}

func (d *FileDecoder) DecodeHeader() (encoding.BlockHeader, error) {
	for len(d.files) > 0 || d.currentFile != nil {
		if d.currentFile == nil {
			err := d.nextFile()
			if err != nil {
				return encoding.BlockHeader{}, err
			}
		}

		header, err := d.decoder.DecodeHeader()

		switch {
		case err == nil:
			return header, nil
		case err == io.EOF:
			d.Close()
		case err != io.EOF:
			return encoding.BlockHeader{}, err
		}
	}
	return encoding.BlockHeader{}, io.EOF
}

func (d *FileDecoder) DecodeBlock() ([][]uint64, error) {
	values, err := d.decoder.DecodeBlock()

	if err == io.EOF {
		return nil, io.ErrUnexpectedEOF
	}

	if err != nil {
		return nil, err
	}

	return values, nil
}

func (d *FileDecoder) Close() {
	if d.currentFile != nil {
		d.currentFile.Close()
		d.currentFile = nil
	}
}

func NewFileDecoder(files []*DataFile, need []bool) FileDecoder {
	f := FileDecoder{
		files:       files,
		decoder:     encoding.NewDecoder(),
		currentFile: nil,
	}

	f.decoder.Need = need

	return f
}
