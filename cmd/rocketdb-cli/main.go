package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nitinankad/rocketdb/internal/transport"
)

const defaultGateway = "127.0.0.1:8080"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	cmd := os.Args[1]
	switch cmd {
	case "put":
		runPut(os.Args[2:])
	case "get":
		runGet(os.Args[2:])
	case "del":
		runDelete(os.Args[2:])
	case "scan":
		runScan(os.Args[2:])
	case "query":
		runQuery(os.Args[2:])
	case "query-gsi":
		runQueryGSI(os.Args[2:])
	case "table-upsert":
		runTableUpsert(os.Args[2:])
	case "streams":
		runStreams(os.Args[2:])
	case "sql":
		runSQL(os.Args[2:])
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", cmd)
		printUsage()
		os.Exit(2)
	}
}

func runPut(args []string) {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	gateway := fs.String("gateway", defaultGateway, "gateway address")
	table := fs.String("table", "", "table name")
	key := fs.String("key", "", "record key")
	sortKey := fs.String("sort-key", "", "optional sort key")
	value := fs.String("value", "", "record value")
	item := fs.String("item", "", "json object of dynamic attributes")
	_ = fs.Parse(args)

	require(*table != "", "--table is required")
	require(*key != "", "--key is required")

	reqPayload := map[string]any{
		"table": *table,
		"key":   *key,
	}
	if *sortKey != "" {
		reqPayload["sort_key"] = *sortKey
	}
	if strings.TrimSpace(*item) != "" {
		var itemObj map[string]any
		must(json.Unmarshal([]byte(*item), &itemObj))
		reqPayload["item"] = itemObj
	} else {
		reqPayload["value"] = *value
	}

	body, err := json.Marshal(reqPayload)
	must(err)

	status, payload, err := request(*gateway, transport.Request{Method: "PUT", Path: "/v1/kv", Body: body})
	must(err)
	printResponse(status, payload)
}

func runGet(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	gateway := fs.String("gateway", defaultGateway, "gateway address")
	table := fs.String("table", "", "table name")
	key := fs.String("key", "", "record key")
	sortKey := fs.String("sort-key", "", "optional sort key")
	consistency := fs.String("consistency", "strong", "read consistency: strong|eventual")
	_ = fs.Parse(args)

	require(*table != "", "--table is required")
	require(*key != "", "--key is required")
	require(*consistency == "strong" || *consistency == "eventual", "--consistency must be strong or eventual")

	status, payload, err := request(*gateway, transport.Request{
		Method: "GET",
		Path:   "/v1/kv",
		Query: map[string]string{
			"table":       *table,
			"key":         *key,
			"sort_key":    *sortKey,
			"consistency": *consistency,
		},
	})
	must(err)
	printResponse(status, payload)
}

func runDelete(args []string) {
	fs := flag.NewFlagSet("del", flag.ExitOnError)
	gateway := fs.String("gateway", defaultGateway, "gateway address")
	table := fs.String("table", "", "table name")
	key := fs.String("key", "", "record key")
	sortKey := fs.String("sort-key", "", "optional sort key")
	_ = fs.Parse(args)

	require(*table != "", "--table is required")
	require(*key != "", "--key is required")

	body, err := json.Marshal(map[string]string{
		"table":    *table,
		"key":      *key,
		"sort_key": *sortKey,
	})
	must(err)

	status, payload, err := request(*gateway, transport.Request{Method: "DELETE", Path: "/v1/kv", Body: body})
	must(err)
	printResponse(status, payload)
}

func runScan(args []string) {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	gateway := fs.String("gateway", defaultGateway, "gateway address")
	table := fs.String("table", "", "table name")
	limit := fs.Int("limit", 50, "page size")
	cursor := fs.String("cursor", "", "pagination cursor")
	consistency := fs.String("consistency", "strong", "read consistency: strong|eventual")
	_ = fs.Parse(args)

	require(*table != "", "--table is required")
	require(*consistency == "strong" || *consistency == "eventual", "--consistency must be strong or eventual")

	body, err := json.Marshal(map[string]any{
		"table":       *table,
		"limit":       *limit,
		"cursor":      *cursor,
		"consistency": *consistency,
	})
	must(err)

	status, payload, err := request(*gateway, transport.Request{Method: "POST", Path: "/v1/scan", Body: body})
	must(err)
	printResponse(status, payload)
}

func runQuery(args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	gateway := fs.String("gateway", defaultGateway, "gateway address")
	table := fs.String("table", "", "table name")
	key := fs.String("key", "", "partition key")
	sortOp := fs.String("sort-op", "", "sort op: any|eq|begins_with|gt|gte|lt|lte")
	sortValue := fs.String("sort-value", "", "sort comparison value")
	limit := fs.Int("limit", 100, "page size")
	cursor := fs.String("cursor", "", "pagination cursor")
	consistency := fs.String("consistency", "strong", "read consistency: strong|eventual")
	_ = fs.Parse(args)

	require(*table != "", "--table is required")
	require(*key != "", "--key is required")

	body, err := json.Marshal(map[string]any{
		"table":       *table,
		"key":         *key,
		"sort_op":     *sortOp,
		"sort_value":  *sortValue,
		"limit":       *limit,
		"cursor":      *cursor,
		"consistency": *consistency,
	})
	must(err)
	status, payload, err := request(*gateway, transport.Request{Method: "POST", Path: "/v1/query", Body: body})
	must(err)
	printResponse(status, payload)
}

func runQueryGSI(args []string) {
	fs := flag.NewFlagSet("query-gsi", flag.ExitOnError)
	gateway := fs.String("gateway", defaultGateway, "gateway address")
	table := fs.String("table", "", "table name")
	index := fs.String("index", "gsi1", "index name")
	pkValue := fs.String("pk-value", "", "gsi partition value")
	skOp := fs.String("sk-op", "", "sort op: any|eq|begins_with|gt|gte|lt|lte")
	skValue := fs.String("sk-value", "", "gsi sort comparison value")
	limit := fs.Int("limit", 100, "page size")
	cursor := fs.String("cursor", "", "pagination cursor")
	consistency := fs.String("consistency", "strong", "read consistency: strong|eventual")
	_ = fs.Parse(args)

	require(*table != "", "--table is required")
	require(*pkValue != "", "--pk-value is required")

	body, err := json.Marshal(map[string]any{
		"table":       *table,
		"index":       *index,
		"pk_value":    *pkValue,
		"sk_op":       *skOp,
		"sk_value":    *skValue,
		"limit":       *limit,
		"cursor":      *cursor,
		"consistency": *consistency,
	})
	must(err)
	status, payload, err := request(*gateway, transport.Request{Method: "POST", Path: "/v1/query-gsi", Body: body})
	must(err)
	printResponse(status, payload)
}

func runTableUpsert(args []string) {
	fs := flag.NewFlagSet("table-upsert", flag.ExitOnError)
	gateway := fs.String("gateway", defaultGateway, "gateway address")
	name := fs.String("name", "", "table name")
	pk := fs.String("pk", "key", "partition key field name")
	sk := fs.String("sk", "", "sort key field name")
	gsiName := fs.String("gsi-name", "gsi1", "single GSI name")
	gsiPK := fs.String("gsi-pk-attr", "gsi1pk", "GSI partition attribute")
	gsiSK := fs.String("gsi-sk-attr", "gsi1sk", "GSI sort attribute (optional)")
	ttlAttr := fs.String("ttl-attr", "ttl_epoch", "TTL attribute name")
	_ = fs.Parse(args)

	require(*name != "", "--name is required")

	body, err := json.Marshal(map[string]any{
		"name":               *name,
		"partition_key":      *pk,
		"sort_key":           *sk,
		"gsi1_name":          *gsiName,
		"gsi1_pk_attribute":  *gsiPK,
		"gsi1_sk_attribute":  *gsiSK,
		"ttl_attribute":      *ttlAttr,
		"replication_factor": 2,
	})
	must(err)
	status, payload, err := request(*gateway, transport.Request{Method: "POST", Path: "/v1/admin/tables/upsert", Body: body})
	must(err)
	printResponse(status, payload)
}

func runStreams(args []string) {
	fs := flag.NewFlagSet("streams", flag.ExitOnError)
	gateway := fs.String("gateway", defaultGateway, "gateway address")
	cursor := fs.Int64("cursor", 0, "stream cursor")
	limit := fs.Int("limit", 100, "max events")
	_ = fs.Parse(args)

	status, payload, err := request(*gateway, transport.Request{
		Method: "GET",
		Path:   "/v1/streams",
		Query: map[string]string{
			"cursor": strconv.FormatInt(*cursor, 10),
			"limit":  strconv.Itoa(*limit),
		},
	})
	must(err)
	printResponse(status, payload)
}

func runSQL(args []string) {
	fs := flag.NewFlagSet("sql", flag.ExitOnError)
	gateway := fs.String("gateway", defaultGateway, "gateway address")
	_ = fs.Parse(args)

	query := strings.TrimSpace(strings.Join(fs.Args(), " "))
	require(query != "", "sql query is required")

	selectScanRe := regexp.MustCompile(`(?i)^\s*select\s+\*\s+from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:limit\s+([0-9]+))?\s*(?:consistency\s+(strong|eventual))?\s*;?\s*$`)
	selectGetRe := regexp.MustCompile(`(?i)^\s*select\s+\*\s+from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+where\s+key\s*=\s*'([^']+)'\s*(?:consistency\s+(strong|eventual))?\s*;?\s*$`)
	insertRe := regexp.MustCompile(`(?is)^\s*insert\s+into\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.+)\)\s*values\s*\((.+)\)\s*;?\s*$`)
	deleteRe := regexp.MustCompile(`(?i)^\s*delete\s+from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+where\s+key\s*=\s*'([^']+)'\s*;?\s*$`)

	if m := selectGetRe.FindStringSubmatch(query); m != nil {
		table := m[1]
		key := m[2]
		consistency := "strong"
		if len(m) > 3 && m[3] != "" {
			consistency = strings.ToLower(m[3])
		}
		status, payload, err := request(*gateway, transport.Request{
			Method: "GET",
			Path:   "/v1/kv",
			Query: map[string]string{
				"table":       table,
				"key":         key,
				"consistency": consistency,
			},
		})
		must(err)
		printResponse(status, payload)
		return
	}

	if m := selectScanRe.FindStringSubmatch(query); m != nil {
		table := m[1]
		limit := 50
		if len(m) > 2 && m[2] != "" {
			parsed, err := strconv.Atoi(m[2])
			must(err)
			limit = parsed
		}
		consistency := "strong"
		if len(m) > 3 && m[3] != "" {
			consistency = strings.ToLower(m[3])
		}
		body, err := json.Marshal(map[string]any{
			"table":       table,
			"limit":       limit,
			"consistency": consistency,
		})
		must(err)
		status, payload, err := request(*gateway, transport.Request{Method: "POST", Path: "/v1/scan", Body: body})
		must(err)
		printResponse(status, payload)
		return
	}

	if m := insertRe.FindStringSubmatch(query); m != nil {
		table := m[1]
		cols, err := splitSQLList(m[2])
		must(err)
		valTokens, err := splitSQLList(m[3])
		must(err)
		require(len(cols) == len(valTokens), "insert columns and values count must match")

		item := make(map[string]any, len(cols))
		key := ""
		sortKey := ""
		for i := range cols {
			col := strings.TrimSpace(cols[i])
			require(col != "", "insert column cannot be empty")

			val, err := parseSQLLiteral(valTokens[i])
			must(err)
			if strings.EqualFold(col, "key") {
				key = fmt.Sprint(val)
				continue
			}
			if strings.EqualFold(col, "sort_key") {
				sortKey = fmt.Sprint(val)
				continue
			}
			item[col] = val
		}
		require(key != "", "insert must include key column")

		body, err := json.Marshal(map[string]any{
			"table":    table,
			"key":      key,
			"sort_key": sortKey,
			"item":     item,
		})
		must(err)
		status, payload, err := request(*gateway, transport.Request{Method: "PUT", Path: "/v1/kv", Body: body})
		must(err)
		printResponse(status, payload)
		return
	}

	if m := deleteRe.FindStringSubmatch(query); m != nil {
		table := m[1]
		key := m[2]
		body, err := json.Marshal(map[string]string{
			"table": table,
			"key":   key,
		})
		must(err)
		status, payload, err := request(*gateway, transport.Request{Method: "DELETE", Path: "/v1/kv", Body: body})
		must(err)
		printResponse(status, payload)
		return
	}

	fmt.Fprintln(os.Stderr, "unsupported SQL. Supported:")
	fmt.Fprintln(os.Stderr, "  SELECT * FROM <table> [LIMIT n] [CONSISTENCY strong|eventual]")
	fmt.Fprintln(os.Stderr, "  SELECT * FROM <table> WHERE key = '<k>' [CONSISTENCY strong|eventual]")
	fmt.Fprintln(os.Stderr, "  INSERT INTO <table> (key, col1, col2, ...) VALUES ('<k>', v1, v2, ...)")
	fmt.Fprintln(os.Stderr, "  DELETE FROM <table> WHERE key = '<k>'")
	os.Exit(2)
}

func splitSQLList(s string) ([]string, error) {
	out := make([]string, 0, 4)
	var b strings.Builder
	inQuote := false

	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' {
			// SQL escaping: '' inside a quoted string.
			if inQuote && i+1 < len(s) && s[i+1] == '\'' {
				b.WriteByte('\'')
				i++
				continue
			}
			inQuote = !inQuote
			b.WriteByte(ch)
			continue
		}
		if ch == ',' && !inQuote {
			out = append(out, strings.TrimSpace(b.String()))
			b.Reset()
			continue
		}
		b.WriteByte(ch)
	}
	if inQuote {
		return nil, fmt.Errorf("unterminated quoted string in sql list")
	}
	last := strings.TrimSpace(b.String())
	if last != "" {
		out = append(out, last)
	}
	return out, nil
}

func parseSQLLiteral(token string) (any, error) {
	t := strings.TrimSpace(token)
	if len(t) >= 2 && t[0] == '\'' && t[len(t)-1] == '\'' {
		return strings.ReplaceAll(t[1:len(t)-1], "''", "'"), nil
	}
	lower := strings.ToLower(t)
	switch lower {
	case "null":
		return nil, nil
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	if i, err := strconv.ParseInt(t, 10, 64); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(t, 64); err == nil {
		return f, nil
	}
	return nil, fmt.Errorf("unsupported SQL literal: %s", token)
}

func request(addr string, req transport.Request) (int, []byte, error) {
	client := transport.NewClient(addr, 3*time.Second, 1)
	defer client.Close()

	resp, err := client.Do(context.Background(), req)
	if err != nil {
		return 0, nil, err
	}
	return resp.Status, resp.Body, nil
}

func printResponse(status int, payload []byte) {
	fmt.Fprintf(os.Stderr, "status=%d\n", status)

	var out bytes.Buffer
	if err := json.Indent(&out, payload, "", "  "); err == nil {
		fmt.Println(out.String())
	} else {
		fmt.Println(string(payload))
	}

	if status >= 400 {
		os.Exit(1)
	}
}

func require(ok bool, msg string) {
	if ok {
		return
	}
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(2)
}

func must(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func printUsage() {
	fmt.Println("rocketdb-cli: simple gateway client")
	fmt.Println("")
	fmt.Println("Usage:")
	fmt.Println("  rocketdb-cli put  --table users --key user-1 [--value 'Ada'] [--item '{\"name\":\"Ada\",\"age\":42}'] [--gateway 127.0.0.1:8080]")
	fmt.Println("  rocketdb-cli get  --table users --key user-1 [--consistency strong|eventual] [--gateway 127.0.0.1:8080]")
	fmt.Println("  rocketdb-cli del  --table users --key user-1 [--gateway 127.0.0.1:8080]")
	fmt.Println("  rocketdb-cli scan --table users [--limit 50] [--cursor '<cursor>'] [--consistency strong|eventual] [--gateway 127.0.0.1:8080]")
	fmt.Println("  rocketdb-cli query --table users --key p1 [--sort-op begins_with --sort-value 2026-] [--gateway 127.0.0.1:8080]")
	fmt.Println("  rocketdb-cli query-gsi --table users --index by_email --pk-value a@x.com [--sk-op gte --sk-value 0]")
	fmt.Println("  rocketdb-cli table-upsert --name users --pk key --sk sort_key --gsi-name by_email --gsi-pk-attr email --gsi-sk-attr created_at")
	fmt.Println("  rocketdb-cli streams [--cursor 0] [--limit 100]")
	fmt.Println("  rocketdb-cli sql  \"insert into users (key, name, age) values ('u1','Ada',42)\" [--gateway 127.0.0.1:8080]")
}
