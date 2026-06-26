package storage

import (
	"encoding/binary"
	"fmt"
	"os"
)

// entrySize — размер одной записи индекса в байтах: [offset:8][position:8]
const entrySize = 16

var ErrIndexEmpty = fmt.Errorf("index is empty")
var ErrOffsetNotFound = fmt.Errorf("offset not found")

// Index управляет файлом смещений для одного сегмента.
type Index struct {
	file   *os.File
	size   int64
	closed bool
}

// OpenIndex открывает или создаёт индексный файл по заданному пути.
func OpenIndex(path string) (*Index, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &Index{
		file: f,
		size: info.Size(),
	}, nil
}

// Append добавляет запись [offset → position] в конец индекса.
func (idx *Index) Append(offset int64, position int64) error {
	buf := make([]byte, entrySize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(offset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(position))

	if _, err := idx.file.Write(buf); err != nil {
		return fmt.Errorf("index append failed: %w", err)
	}

	idx.size += entrySize
	return nil
}

// Lookup ищет позицию в .log файле для заданного offset.
// Использует бинарный поиск — работает за O(log n).
func (idx *Index) Lookup(offset int64) (int64, error) {
	count := idx.Count()
	if count == 0 {
		return 0, ErrIndexEmpty
	}

	lo, hi := int64(0), count-1
	for lo <= hi {
		mid := (lo + hi) / 2
		idxOffset, position, err := idx.readAt(mid)
		if err != nil {
			return 0, err
		}

		if idxOffset == offset {
			return position, nil
		} else if idxOffset < offset {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	return 0, fmt.Errorf("offset %d not found in index", offset)
}

// Count возвращает количество записей в индексе.
func (idx *Index) Count() int64 {
	return idx.size / entrySize
}

// LastOffset возвращает последнее смещение в индексе.
// Используется при восстановлении сегмента после перезапуска.
func (idx *Index) LastOffset(baseOffset int64) int64 {
	count := idx.Count()
	if count == 0 {
		return baseOffset
	}
	idxOffset, _, err := idx.readAt(count - 1)
	if err != nil {
		return baseOffset
	}
	return idxOffset + 1
}

// IsOpen возвращает true если файл индекса открыт.
func (idx *Index) IsOpen() bool {
	return !idx.closed
}

// Close закрывает файл индекса.
func (idx *Index) Close() error {
	idx.closed = true
	return idx.file.Close()
}

// readAt читает запись индекса по порядковому номеру (не байтовой позиции).
func (idx *Index) readAt(n int64) (offset int64, position int64, err error) {
	if idx.closed {
		return 0, 0, fmt.Errorf("index is closed")
	}
	buf := make([]byte, entrySize)
	if _, err = idx.file.ReadAt(buf, n*entrySize); err != nil {
		return 0, 0, fmt.Errorf("index read at %d failed: %w", n, err)
	}
	offset = int64(binary.BigEndian.Uint64(buf[0:8]))
	position = int64(binary.BigEndian.Uint64(buf[8:16]))
	return offset, position, nil
}
