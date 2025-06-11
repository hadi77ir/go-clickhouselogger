package clickhouselogger

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/hadi77ir/go-logging"
)

// Log represents the structure of the event to be logged
type Log struct {
	Timestamp  time.Time
	Level      string
	Message    string
	ResourceID string
}

type LogWriter struct {
	client     clickhouse.Conn
	resourceId string
}

var levelMap = map[logging.Level]string{
	logging.TraceLevel: "trace",
	logging.DebugLevel: "debug",
	logging.InfoLevel:  "info",
	logging.WarnLevel:  "warn",
	logging.ErrorLevel: "error",
	logging.FatalLevel: "fatal",
	logging.PanicLevel: "panic",
}

func NewLogWriter(connection string, resourceId string) (*LogWriter, error) {
	conn, err := url.Parse(connection)
	if err != nil {
		return nil, err
	}
	var username string
	var password string
	if conn.User != nil {
		username = conn.User.Username()
		password, _ = conn.User.Password()
	}
	client, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{conn.Host},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(conn.Path, "/"),
			Username: username,
			Password: password,
		},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return nil, err
	}

	// try creating table if it doesn't exist
	query := `
		CREATE TABLE IF NOT EXISTS logs (
			timestamp DateTime64(9),
			level String,
			message String,
		    fields String,
			resource_id String
		) ENGINE = MergeTree()
		ORDER BY timestamp
	`
	err = client.Exec(context.Background(), query)
	if err != nil {
		return nil, err
	}

	writer := &LogWriter{
		client:     client,
		resourceId: resourceId,
	}
	return writer, nil
}

func (w *LogWriter) Write(level logging.Level, args []any, fields logging.Fields) error {
	query := `
		INSERT INTO logs (timestamp, level, message, fields, resource_id)
		VALUES (?, ?, ?, ?, ?)
	`
	return w.client.Exec(context.Background(), query,
		time.Now(), levelMap[level], fmt.Sprint(args), stringifyFields(fields), w.resourceId)
}

func stringifyFields(fields logging.Fields) string {
	if len(fields) == 0 {
		return ""
	}
	b := &strings.Builder{}
	for k, v := range fields {
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(fmt.Sprint(v))
		b.WriteString("\n")
	}
	return b.String()
}

type Logger struct {
	writer *LogWriter
	fields logging.Fields
}

func (l *Logger) Log(level logging.Level, args ...interface{}) {
	_ = l.writer.Write(level, args, l.fields)

	if level == logging.FatalLevel {
		os.Exit(1)
	}
	if level == logging.PanicLevel {
		panic(fmt.Sprint(args...))
	}
}

func (l *Logger) WithFields(fields logging.Fields) logging.Logger {
	return &Logger{
		writer: l.writer,
		fields: fields,
	}
}

func (l *Logger) WithAdditionalFields(fields logging.Fields) logging.Logger {
	merged := fields
	for k, v := range l.fields {
		if _, ok := merged[k]; !ok {
			merged[k] = v
		}
	}
	return l.WithFields(merged)
}

func (l *Logger) Logger() logging.Logger {
	return &Logger{writer: l.writer}
}

func NewLogger(connection, resourceId string) (logging.Logger, error) {
	writer, err := NewLogWriter(connection, resourceId)
	if err != nil {
		return nil, err
	}
	return &Logger{
		writer: writer,
	}, nil
}

var _ logging.Logger = &Logger{}
