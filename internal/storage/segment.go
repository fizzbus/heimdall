package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Entry — одна запись в журнале.
type Entry struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// Формат бинарной записи в .log файле:
//
//	[offset:8][timestamp:8][keyLen:4][valueLen:4][key][value]
const headerSize = 8 + 8 + 4 + 4 // 24 байта

// Segment представляет пару файлов .log и .index.
type Segment struct {
	mu         sync.Mutex
	dir        string
	baseOffset int64
	maxBytes   int64

	logFile *os.File
	index   *Index
	writer  *bufio.Writer

	logSize    int64
	nextOffset int64
}

// NewSegment создаёт новый сегмент с заданным базовым смещением.
func NewSegment(dir string, baseOffset int64, maxBytes int64) (*Segment, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	baseName := filepath.Join(dir, fmt.Sprintf("%020d", baseOffset))

	logFile, err := os.OpenFile(baseName+".log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	index, err := OpenIndex(baseName + ".index")
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to open index: %w", err)
	}

	info, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		index.Close()
		return nil, err
	}

	nextOffset := index.LastOffset(baseOffset)

	return &Segment{
		dir:        dir,
		baseOffset: baseOffset,
		maxBytes:   maxBytes,
		logFile:    logFile,
		index:      index,
		writer:     bufio.NewWriterSize(logFile, 64*1024),
		logSize:    info.Size(),
		nextOffset: nextOffset,
	}, nil
}

// Write записывает Entry в .log и добавляет запись в .index.
func (s *Segment) Write(e Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	position := s.logSize

	// Заголовок
	header := make([]byte, headerSize)
	binary.BigEndian.PutUint64(header[0:8], uint64(e.Offset))
	binary.BigEndian.PutUint64(header[8:16], uint64(e.Timestamp))
	binary.BigEndian.PutUint32(header[16:20], uint32(len(e.Key)))
	binary.BigEndian.PutUint32(header[20:24], uint32(len(e.Value)))

	if _, err := s.writer.Write(header); err != nil {
		return err
	}
	if _, err := s.writer.Write(e.Key); err != nil {
		return err
	}
	if _, err := s.writer.Write(e.Value); err != nil {
		return err
	}
	if err := s.writer.Flush(); err != nil {
		return err
	}

	s.logSize += int64(headerSize + len(e.Key) + len(e.Value))

	// Запись в индекс
	if err := s.index.Append(e.Offset, position); err != nil {
		return err
	}

	s.nextOffset++
	return nil
}

// Read читает записи из сегмента начиная с offset, суммарно не более maxBytes.
func (s *Segment) Read(offset int64, maxBytes int) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	position, err := s.index.Lookup(offset)
	if err != nil {
		return nil, err
	}

	if _, err := s.logFile.Seek(position, io.SeekStart); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(s.logFile)
	var entries []Entry
	bytesRead := 0

	for bytesRead < maxBytes {
		header := make([]byte, headerSize)
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		msgOffset := int64(binary.BigEndian.Uint64(header[0:8]))
		timestamp := int64(binary.BigEndian.Uint64(header[8:16]))
		keyLen := int(binary.BigEndian.Uint32(header[16:20]))
		valueLen := int(binary.BigEndian.Uint32(header[20:24]))

		key := make([]byte, keyLen)
		value := make([]byte, valueLen)

		if _, err := io.ReadFull(reader, key); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}

		entries = append(entries, Entry{
			Offset:    msgOffset,
			Timestamp: timestamp,
			Key:       key,
			Value:     value,
		})

		bytesRead += headerSize + keyLen + valueLen
	}

	return entries, nil
}

// IsFull возвращает true, если лог-файл достиг максимального размера.
func (s *Segment) IsFull() bool {
	return s.logSize >= s.maxBytes
}

// BaseOffset возвращает базовое смещение сегмента.
func (s *Segment) BaseOffset() int64 {
	return s.baseOffset
}

// NextOffset возвращает следующее свободное смещение.
func (s *Segment) NextOffset() int64 {
	return s.nextOffset
}

// Dir возвращает директорию сегмента.
func (s *Segment) Dir() string {
	return s.dir
}

// Close сбрасывает буфер и закрывает файлы.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.writer.Flush(); err != nil {
		return err
	}
	if err := s.logFile.Close(); err != nil {
		return err
	}
	return s.index.Close()
}

// LoadSegments загружает все существующие сегменты из директории.
func LoadSegments(dir string, maxBytes int64) ([]*Segment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var baseOffsets []int64
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			var base int64
			fmt.Sscanf(e.Name(), "%d.log", &base)
			baseOffsets = append(baseOffsets, base)
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	segments := make([]*Segment, 0, len(baseOffsets))
	for _, base := range baseOffsets {
		seg, err := NewSegment(dir, base, maxBytes)
		if err != nil {
			return nil, err
		}
		segments = append(segments, seg)
	}

	return segments, nil
}
