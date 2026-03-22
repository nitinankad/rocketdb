package storage

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Disk struct {
	mu      sync.RWMutex
	path    string
	walPath string
	walFile *os.File
	tables  map[string]map[string][]byte
}

type diskSnapshot struct {
	Tables map[string]map[string]string `json:"tables"`
}

type walRecord struct {
	Op        string `json:"op"`
	Table     string `json:"table"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
	Timestamp int64  `json:"timestamp_unix_nano"`
}

func NewDisk(path string) (*Disk, error) {
	if path == "" {
		return nil, fmt.Errorf("path is required")
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}

	d := &Disk{
		path:    path,
		walPath: path + ".wal",
		tables:  make(map[string]map[string][]byte),
	}

	if err := d.loadLegacySnapshot(); err != nil {
		return nil, err
	}
	if err := d.replayWAL(); err != nil {
		return nil, err
	}
	if err := d.openWAL(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Disk) Put(_ context.Context, table, key string, value []byte) error {
	if table == "" || key == "" {
		return fmt.Errorf("table and key are required")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.tables[table]; !ok {
		d.tables[table] = make(map[string][]byte)
	}

	record := walRecord{
		Op:        "put",
		Table:     table,
		Key:       key,
		Value:     base64.StdEncoding.EncodeToString(value),
		Timestamp: time.Now().UnixNano(),
	}
	if err := d.appendWALLocked(record); err != nil {
		return err
	}

	copied := make([]byte, len(value))
	copy(copied, value)
	d.tables[table][key] = copied

	return nil
}

func (d *Disk) Get(_ context.Context, table, key string) ([]byte, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	tableData, ok := d.tables[table]
	if !ok {
		return nil, false, nil
	}
	val, ok := tableData[key]
	if !ok {
		return nil, false, nil
	}

	copied := make([]byte, len(val))
	copy(copied, val)
	return copied, true, nil
}

func (d *Disk) Delete(_ context.Context, table, key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if tableData, ok := d.tables[table]; ok {
		record := walRecord{
			Op:        "delete",
			Table:     table,
			Key:       key,
			Timestamp: time.Now().UnixNano(),
		}
		if err := d.appendWALLocked(record); err != nil {
			return err
		}
		delete(tableData, key)
	}
	return nil
}

func (d *Disk) Scan(_ context.Context, table, cursor string, limit int) ([]Row, string, error) {
	if limit <= 0 {
		return []Row{}, "", nil
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	tableData, ok := d.tables[table]
	if !ok || len(tableData) == 0 {
		return []Row{}, "", nil
	}

	keys := make([]string, 0, len(tableData))
	for key := range tableData {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	start := 0
	if cursor != "" {
		idx := sort.SearchStrings(keys, cursor)
		for idx < len(keys) && keys[idx] <= cursor {
			idx++
		}
		start = idx
	}
	if start >= len(keys) {
		return []Row{}, "", nil
	}

	end := start + limit
	if end > len(keys) {
		end = len(keys)
	}

	rows := make([]Row, 0, end-start)
	for _, key := range keys[start:end] {
		val := tableData[key]
		copied := make([]byte, len(val))
		copy(copied, val)
		rows = append(rows, Row{
			Key:   key,
			Value: copied,
		})
	}

	nextCursor := ""
	if end < len(keys) {
		nextCursor = keys[end-1]
	}

	return rows, nextCursor, nil
}

func (d *Disk) loadLegacySnapshot() error {
	raw, err := os.ReadFile(d.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read data file: %w", err)
	}

	var snap diskSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return fmt.Errorf("decode data file: %w", err)
	}

	d.tables = make(map[string]map[string][]byte, len(snap.Tables))
	for table, rows := range snap.Tables {
		d.tables[table] = make(map[string][]byte, len(rows))
		for key, encoded := range rows {
			val, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				return fmt.Errorf("decode value for %s/%s: %w", table, key, err)
			}
			d.tables[table][key] = val
		}
	}

	return nil
}

func (d *Disk) replayWAL() error {
	f, err := os.Open(d.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open wal file: %w", err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("read wal: %w", err)
		}

		trimmed := bytesTrimSpace(line)
		if len(trimmed) > 0 {
			var rec walRecord
			if uErr := json.Unmarshal(trimmed, &rec); uErr != nil {
				return fmt.Errorf("decode wal record: %w", uErr)
			}
			if aErr := d.applyWALRecord(rec); aErr != nil {
				return aErr
			}
		}

		if err == io.EOF {
			break
		}
	}

	return nil
}

func (d *Disk) openWAL() error {
	f, err := os.OpenFile(d.walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open wal for append: %w", err)
	}
	d.walFile = f
	return nil
}

func (d *Disk) appendWALLocked(rec walRecord) error {
	if d.walFile == nil {
		return fmt.Errorf("wal is not initialized")
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("encode wal record: %w", err)
	}
	raw = append(raw, '\n')
	if _, err := d.walFile.Write(raw); err != nil {
		return fmt.Errorf("append wal record: %w", err)
	}
	if err := d.walFile.Sync(); err != nil {
		return fmt.Errorf("sync wal record: %w", err)
	}
	return nil
}

func (d *Disk) applyWALRecord(rec walRecord) error {
	switch rec.Op {
	case "put":
		decoded, err := base64.StdEncoding.DecodeString(rec.Value)
		if err != nil {
			return fmt.Errorf("decode wal value for %s/%s: %w", rec.Table, rec.Key, err)
		}
		if _, ok := d.tables[rec.Table]; !ok {
			d.tables[rec.Table] = make(map[string][]byte)
		}
		copied := make([]byte, len(decoded))
		copy(copied, decoded)
		d.tables[rec.Table][rec.Key] = copied
	case "delete":
		if rows, ok := d.tables[rec.Table]; ok {
			delete(rows, rec.Key)
		}
	default:
		return fmt.Errorf("unknown wal op: %s", rec.Op)
	}
	return nil
}

func bytesTrimSpace(b []byte) []byte {
	start := 0
	for start < len(b) && (b[start] == ' ' || b[start] == '\n' || b[start] == '\r' || b[start] == '\t') {
		start++
	}
	end := len(b)
	for end > start && (b[end-1] == ' ' || b[end-1] == '\n' || b[end-1] == '\r' || b[end-1] == '\t') {
		end--
	}
	return b[start:end]
}
