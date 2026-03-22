package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const defaultGateway = "http://localhost:8080"

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
	gateway := fs.String("gateway", defaultGateway, "gateway base URL")
	table := fs.String("table", "", "table name")
	key := fs.String("key", "", "record key")
	value := fs.String("value", "", "record value")
	item := fs.String("item", "", "json object of dynamic attributes")
	_ = fs.Parse(args)

	require(*table != "", "--table is required")
	require(*key != "", "--key is required")

	reqPayload := map[string]any{
		"table": *table,
		"key":   *key,
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

	status, payload, err := request(http.MethodPut, strings.TrimRight(*gateway, "/")+"/v1/kv", body)
	must(err)
	printResponse(status, payload)
}

func runGet(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	gateway := fs.String("gateway", defaultGateway, "gateway base URL")
	table := fs.String("table", "", "table name")
	key := fs.String("key", "", "record key")
	consistency := fs.String("consistency", "strong", "read consistency: strong|eventual")
	_ = fs.Parse(args)

	require(*table != "", "--table is required")
	require(*key != "", "--key is required")
	require(*consistency == "strong" || *consistency == "eventual", "--consistency must be strong or eventual")

	target := fmt.Sprintf(
		"%s/v1/kv?table=%s&key=%s&consistency=%s",
		strings.TrimRight(*gateway, "/"),
		url.QueryEscape(*table),
		url.QueryEscape(*key),
		url.QueryEscape(*consistency),
	)

	status, payload, err := request(http.MethodGet, target, nil)
	must(err)
	printResponse(status, payload)
}

func runDelete(args []string) {
	fs := flag.NewFlagSet("del", flag.ExitOnError)
	gateway := fs.String("gateway", defaultGateway, "gateway base URL")
	table := fs.String("table", "", "table name")
	key := fs.String("key", "", "record key")
	_ = fs.Parse(args)

	require(*table != "", "--table is required")
	require(*key != "", "--key is required")

	body, err := json.Marshal(map[string]string{
		"table": *table,
		"key":   *key,
	})
	must(err)

	status, payload, err := request(http.MethodDelete, strings.TrimRight(*gateway, "/")+"/v1/kv", body)
	must(err)
	printResponse(status, payload)
}

func runScan(args []string) {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	gateway := fs.String("gateway", defaultGateway, "gateway base URL")
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

	status, payload, err := request(http.MethodPost, strings.TrimRight(*gateway, "/")+"/v1/scan", body)
	must(err)
	printResponse(status, payload)
}

func runSQL(args []string) {
	fs := flag.NewFlagSet("sql", flag.ExitOnError)
	gateway := fs.String("gateway", defaultGateway, "gateway base URL")
	_ = fs.Parse(args)

	query := strings.TrimSpace(strings.Join(fs.Args(), " "))
	require(query != "", "sql query is required")

	selectScanRe := regexp.MustCompile(`(?i)^\s*select\s+\*\s+from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:limit\s+([0-9]+))?\s*(?:consistency\s+(strong|eventual))?\s*;?\s*$`)
	selectGetRe := regexp.MustCompile(`(?i)^\s*select\s+\*\s+from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+where\s+key\s*=\s*'([^']+)'\s*(?:consistency\s+(strong|eventual))?\s*;?\s*$`)
	insertRe := regexp.MustCompile(`(?is)^\s*insert\s+into\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.+)\)\s*values\s*\((.+)\)\s*;?\s*$`)
	deleteRe := regexp.MustCompile(`(?i)^\s*delete\s+from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+where\s+key\s*=\s*'([^']+)'\s*;?\s*$`)

	base := strings.TrimRight(*gateway, "/")
	if m := selectGetRe.FindStringSubmatch(query); m != nil {
		table := m[1]
		key := m[2]
		consistency := "strong"
		if len(m) > 3 && m[3] != "" {
			consistency = strings.ToLower(m[3])
		}
		target := fmt.Sprintf("%s/v1/kv?table=%s&key=%s&consistency=%s",
			base,
			url.QueryEscape(table),
			url.QueryEscape(key),
			url.QueryEscape(consistency),
		)
		status, payload, err := request(http.MethodGet, target, nil)
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
		status, payload, err := request(http.MethodPost, base+"/v1/scan", body)
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
		for i := range cols {
			col := strings.TrimSpace(cols[i])
			require(col != "", "insert column cannot be empty")

			val, err := parseSQLLiteral(valTokens[i])
			must(err)
			if strings.EqualFold(col, "key") {
				key = fmt.Sprint(val)
				continue
			}
			item[col] = val
		}
		require(key != "", "insert must include key column")

		body, err := json.Marshal(map[string]any{
			"table": table,
			"key":   key,
			"item":  item,
		})
		must(err)
		status, payload, err := request(http.MethodPut, base+"/v1/kv", body)
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
		status, payload, err := request(http.MethodDelete, base+"/v1/kv", body)
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

func request(method, target string, body []byte) (int, []byte, error) {
	client := &http.Client{Timeout: 3 * time.Second}

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, target, reader)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, payload, nil
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
	fmt.Println("  rocketdb-cli put  --table users --key user-1 [--value 'Ada'] [--item '{\"name\":\"Ada\",\"age\":42}'] [--gateway http://localhost:8080]")
	fmt.Println("  rocketdb-cli get  --table users --key user-1 [--consistency strong|eventual] [--gateway http://localhost:8080]")
	fmt.Println("  rocketdb-cli del  --table users --key user-1 [--gateway http://localhost:8080]")
	fmt.Println("  rocketdb-cli scan --table users [--limit 50] [--cursor '<cursor>'] [--consistency strong|eventual] [--gateway http://localhost:8080]")
	fmt.Println("  rocketdb-cli sql  \"insert into users (key, name, age) values ('u1','Ada',42)\" [--gateway http://localhost:8080]")
}
