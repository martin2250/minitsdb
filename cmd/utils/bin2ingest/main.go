package main

import (
	"encoding/binary"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/lineprotocol"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"strings"

	"github.com/martin2250/minitsdb/cmd/minitsdb-server/ingest"
)

type column struct {
	name  string
	phase string
}

var columns = []column{
	{name: "frequency", phase: ""},
	{name: "power", phase: "A"},
	{name: "power", phase: "B"},
	{name: "power", phase: "C"},
	{name: "voltage", phase: "A"},
	{name: "voltage", phase: "B"},
	{name: "voltage", phase: "C"},
	{name: "current", phase: "A"},
	{name: "current", phase: "B"},
	{name: "current", phase: "C"},
	{name: "current", phase: "T"},
	{name: "energy", phase: "A"},
	{name: "energy", phase: "B"},
	{name: "energy", phase: "C"},
	{name: "energy", phase: "T"},
}

// BinBuffer is the main server struct
type BinBuffer struct {
	files       []string
	currentFile *os.File
}

func (buffer *BinBuffer) nextFile() error {
	if len(buffer.files[0]) == 0 {
		return io.EOF
	}

	var err error
	buffer.currentFile, err = os.Open(buffer.files[0])

	if err != nil {
		return err
	}

	// pop first path
	buffer.files = buffer.files[1:]

	return nil
}

// PopPoint is called via RPC by the main minitsdb binary
func (buffer *BinBuffer) PopPoint(arg int, reply *lineprotocol.Point) error {

	if buffer.currentFile == nil {
		return io.EOF
	}

	var bindata struct {
		Time   uint32
		Values [15]float32
	}

	for {
		err := binary.Read(buffer.currentFile, binary.LittleEndian, &bindata)

		if err == io.EOF {
			buffer.currentFile.Close()

			err = buffer.nextFile()

			if err != nil {
				log.Println(err)
				buffer.currentFile = nil
				return err
			}

			continue
		} else if err != nil {
			log.Println(err)
			buffer.currentFile.Close()
			buffer.currentFile = nil
			return err
		}

		break
	}

	reply.Tags = map[string]string{"name": "power", "loc": "main"}

	// change endianness of time because i'm an idiot
	timebuffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(timebuffer, bindata.Time)

	reply.Time = int64(binary.BigEndian.Uint32(timebuffer))

	reply.Values = make([]ingest.Value, len(columns))

	for i := range bindata.Values {
		reply.Values[i].Value = float64(bindata.Values[i])
		reply.Values[i].Tags = map[string]string{"name": columns[i].name}
		if len(columns[i].phase) > 0 {
			reply.Values[i].Tags["phase"] = columns[i].phase
		}
	}

	return nil
}

func main() {
	buffer := BinBuffer{
		files: make([]string, 0),
	}

	binpath := "/home/martin/Desktop/influx_backup_last/power_main/"

	// list all files in bindir
	pathlist, err := ioutil.ReadDir(binpath)

	if err != nil {
		log.Fatalln(err)
	}

	// filter for bin files
	for _, p := range pathlist {
		if strings.HasSuffix(p.Name(), ".bin") {
			buffer.files = append(buffer.files, path.Join(binpath, p.Name()))
		}
	}

	err = buffer.nextFile()

	if err != nil {
		log.Fatalln(err)
	}

	// pop first path
	buffer.files = buffer.files[1:]

	rpc.RegisterName("Buffer", &buffer)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", ":2002")
	if e != nil {
		log.Fatal("listen error:", e)
	}

	http.Serve(l, nil)
}
