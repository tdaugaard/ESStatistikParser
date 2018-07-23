package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type byteOffset struct {
	from int64
	to   int64
}

type fileChunk struct {
	file    string
	offset  byteOffset
	content []byte
}

type fileChunks []fileChunk

type xmlBlockSstatistik struct {
	Ident                    string `xml:"KoeretoejIdent"`
	ArtNummer                int    `xml:"KoeretoejArtNummer"`
	ArtNavn                  string `xml:"KoeretoejArtNavn"`
	RegistreringNummerNummer string `xml:"RegistreringNummerNummer"`
}

type processStatistics struct {
	timeSpentParsingXML    int64
	timeSpentSplittingFile time.Duration
}

var (
	fileToProcess   string
	blocksProcessed int64
	itemsProcessed  int64
	fileProgress    float64
	onlyReadFile    bool
	maxQueueLen     int
	numberOfChunks  int
	stats           processStatistics
	startTime       time.Time
	chunkChannel    chan fileChunk
	wg              sync.WaitGroup
)

const (
	defaultQueueItemsPerCPU = 500
)

func printProcessStatistics() {
	itemsProcessed := atomic.LoadInt64(&itemsProcessed)
	blocksProcessed := atomic.LoadInt64(&blocksProcessed)

	elapsedTime := time.Now().Sub(startTime)
	itemsPerSecond := float64(blocksProcessed) / stats.timeSpentSplittingFile.Seconds()
	blocksPerSecond := float64(itemsProcessed) / time.Since(startTime).Seconds()
	queueSize := len(chunkChannel)

	fmt.Printf(
		"\r[%.02f%% - %s] blocks: %d, items: %d, (%.02f blocks/s, %.02f items/s); read: %.02fs, parse: %.02fs, queue: %d           ",
		fileProgress,
		elapsedTime.Round(time.Second).String(),
		blocksProcessed, itemsProcessed,
		itemsPerSecond, blocksPerSecond,
		float64(stats.timeSpentSplittingFile)/float64(time.Second),
		float64(stats.timeSpentParsingXML)/float64(time.Second),
		queueSize,
	)
}

func init() {
	numCPU := runtime.NumCPU()

	flag.StringVar(&fileToProcess, "f", "", "file to process")
	flag.IntVar(&numberOfChunks, "w", numCPU, "number of workers")
	flag.IntVar(&maxQueueLen, "q", defaultQueueItemsPerCPU, "max queue size per worker")
	flag.BoolVar(&onlyReadFile, "ro", false, "just read the file, don't parse XML")

	flag.Parse()

	maxQueueLen = maxQueueLen * numCPU

	if fileToProcess == "" {
		flag.Usage()
		os.Exit(0)
	}
}

func splitFile() chan fileChunk {
	ch := make(chan fileChunk, maxQueueLen)

	wg.Add(1)
	go func() {
		stat, err := os.Stat(fileToProcess)
		if err != nil {
			log.Fatal(err)
		}

		fh, err := os.Open(fileToProcess)
		if err != nil {
			log.Fatal(err)
		}
		defer fh.Close()
		defer close(ch)
		defer wg.Done()

		startTime = time.Now()

		const bufReadSize = (16 * 1024 * 1024)

		splitStringBegin, splitStringEnd := []byte("<ns:Statistik>"), []byte("</ns:Statistik>")

		splitStringEndLen := len(splitStringEnd)
		indexBegin, indexEnd := 0, 0
		offset := int64(0)

		startTime := time.Now()
		fileSize := stat.Size()

		for {
			buf := make([]byte, bufReadSize)
			nBytesRead, readError := fh.ReadAt(buf, offset)
			if readError != nil && readError != io.EOF {
				log.Fatal(err)
			}

			indexEnd = 0
			fileProgress = float64(offset*100) / float64(fileSize)

			for {
				if indexBegin = bytes.Index(buf[indexEnd:nBytesRead], splitStringBegin); indexBegin == -1 {
					offset += int64(indexEnd)
					break
				}
				indexBegin += indexEnd

				if indexEnd = bytes.Index(buf[indexBegin:nBytesRead], splitStringEnd); indexEnd == -1 {
					// if no end marker found, set offset such that we will re-read with more data
					offset += int64(indexBegin) - 1
					break
				}
				indexEnd += indexBegin + splitStringEndLen

				chunk := fileChunk{
					file: fileToProcess,
					offset: byteOffset{
						from: offset + int64(indexBegin),
						to:   offset + int64(indexEnd),
					},
					content: buf[indexBegin:indexEnd],
				}

				atomic.AddInt64(&blocksProcessed, 1)

				ch <- chunk
			}

			stats.timeSpentSplittingFile = time.Since(startTime)

			if readError == io.EOF {
				break
			}
		}
	}()

	return ch
}

func processChunk(chunk fileChunk) {
	var inElement string

	startTime := time.Now()
	decoder := xml.NewDecoder(bytes.NewReader(chunk.content))

	// Read tokens from the XML document in a stream.
	t, _ := decoder.Token()
	if t == nil {
		return
	}

	parseOK := false

	// Inspect the type of the token just read.
	switch se := t.(type) {
	case xml.StartElement:
		// If we just read a StartElement token
		inElement = se.Name.Local

		// Inspect the type of the token just read.
		switch se := t.(type) {
		case xml.StartElement:
			if inElement == "Statistik" {
				var xmlBlock xmlBlockSstatistik

				// decode a whole chunk of following XML into the
				// variable p which is a Page (se above)
				decoder.DecodeElement(&xmlBlock, &se)

				timeElapsed := time.Since(startTime).Nanoseconds()
				atomic.AddInt64(&stats.timeSpentParsingXML, timeElapsed)
				atomic.AddInt64(&itemsProcessed, 1)

				sha256.Sum256([]byte(xmlBlock.ArtNavn + ":" + xmlBlock.RegistreringNummerNummer + ":" + xmlBlock.Ident))
				parseOK = true
			}
		default:
		}

	default:
	}

	if !parseOK {
		log.Printf("Unable to parse block at %d => %d (%d bytes)\n",
			chunk.offset.from,
			chunk.offset.to,
			chunk.offset.to-chunk.offset.from,
		)
	}
}

func main() {
	fmt.Printf("Processing file using %d threads\n", numberOfChunks)

	statisticsTicker := time.NewTicker(100 * time.Millisecond)
	go func() {
		for range statisticsTicker.C {
			printProcessStatistics()
		}
	}()

	chunkChannel = splitFile()

	sem := make(chan bool, numberOfChunks)

	for chunk := range chunkChannel {
		sem <- true

		if !onlyReadFile {
			wg.Add(1)
			go func(chunk fileChunk) {
				processChunk(chunk)

				wg.Done()
				<-sem
			}(chunk)
		} else {
			<-sem
		}
	}

	for i := 0; i < cap(sem); i++ {
		sem <- true
	}

	wg.Wait()

	statisticsTicker.Stop()

	printProcessStatistics()
	fmt.Println()
}
