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
	_ = fs.Parse(args)

	require(*table != "", "--table is required")
	require(*key != "", "--key is required")

	body, err := json.Marshal(map[string]string{
		"table": *table,
		"key":   *key,
		"value": *value,
	})
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
	fmt.Println("  rocketdb-cli put  --table users --key user-1 --value '{\"name\":\"Ada\"}' [--gateway http://localhost:8080]")
	fmt.Println("  rocketdb-cli get  --table users --key user-1 [--consistency strong|eventual] [--gateway http://localhost:8080]")
	fmt.Println("  rocketdb-cli del  --table users --key user-1 [--gateway http://localhost:8080]")
	fmt.Println("  rocketdb-cli scan --table users [--limit 50] [--cursor '<cursor>'] [--consistency strong|eventual] [--gateway http://localhost:8080]")
}
