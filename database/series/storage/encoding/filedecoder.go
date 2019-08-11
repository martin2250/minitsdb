package encoding

import (
	"io"
	"os"
)

// FileDecoder decodes blocks from multiple files and handles opening / closing files
type FileDecoder struct {
	files       []string // files to be read
	decoder     Decoder
	currentFile *os.File // file that is currently being read (only held for closing)
}

func (d *FileDecoder) nextFile() error {
	if len(d.files) == 0 {
		return io.EOF
	}

	d.Close()

	var err error
	d.currentFile, err = os.Open(d.files[0])

	if err != nil {
		return err
	}

	d.decoder.SetReader(d.currentFile)
	d.files = d.files[1:]

	return nil
}

func (d *FileDecoder) DecodeHeader() (BlockHeader, error) {
	for len(d.files) > 0 || d.currentFile != nil {
		if d.currentFile == nil {
			err := d.nextFile()
			if err != nil {
				return BlockHeader{}, err
			}
		}

		header, err := d.decoder.DecodeHeader()

		switch {
		case err == nil:
			return header, nil
		case err == io.EOF:
			d.Close()
		case err != io.EOF:
			return BlockHeader{}, err
		}
	}
	return BlockHeader{}, io.EOF
}

func (d *FileDecoder) DecodeBlock() ([][]uint64, error) {
	// if not at body, use FileDeocder.DecodeHeader as it reloads files automatically
	if d.decoder.s != stateBody {
		_, err := d.DecodeHeader()
		if err != nil {
			return nil, err
		}
	}
	// decoder is now at stateBody
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

func NewFileDecoder(files []string, columns []int) FileDecoder {
	f := FileDecoder{
		files:       files,
		decoder:     NewDecoder(),
		currentFile: nil,
	}

	f.decoder.Columns = columns

	return f
}
