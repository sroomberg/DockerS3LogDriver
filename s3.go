package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/plugins/logdriver"
	"github.com/docker/docker/daemon/logger"
	"github.com/docker/docker/daemon/logger/loggerutils"
)

const (
	driverName = "s3logdriver"
)

// S3Logger is the logger struct that implements the Docker logger interface.
type S3Logger struct {
	s3Client *s3.S3
	bucket   string
}

// LogOption represents options for configuring the S3 logger.
type LogOption struct {
	S3Bucket string
}

func main() {
	var opts LogOption
	flag.StringVar(&opts.S3Bucket, "s3-bucket", "", "S3 bucket name")
	flag.Parse()

	if opts.S3Bucket == "" {
		fmt.Println("Please provide an S3 bucket name")
		os.Exit(1)
	}

	// Initialize AWS session
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create an S3 client
	s3Client := s3.New(sess)

	// Create S3Logger instance
	s3Logger := &S3Logger{
		s3Client: s3Client,
		bucket:   opts.S3Bucket,
	}

	// Register the logger with Docker
	h := logdriver.NewHandler(s3Logger)
	err := h.ServeUnix(driverName, 0)
	if err != nil {
		fmt.Printf("Error starting the S3 logger: %s\n", err)
		os.Exit(1)
	}
}

// Log is the method called by Docker daemon to stream container logs.
func (l *S3Logger) Log(ctx context.Context, config logger.Message) error {
	if config.Source != "" {
		return nil // skip logs not coming from a container
	}

	containerID := config.ContainerID

	reader, err := l.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(containerID),
	})
	if err != nil {
		return fmt.Errorf("failed to get object from S3: %v", err)
	}
	defer reader.Body.Close()

	scanner := bufio.NewScanner(reader.Body)
	for scanner.Scan() {
		logLine := scanner.Text()

		// Send log line to Docker daemon
		configLine := logger.LogLine{
			Line:     logLine,
			Source:   containerID,
			Partial:  false,
			Timestamp: time.Now(),
		}

		if err := configLine.MarshalJSON(); err != nil {
			return fmt.Errorf("error marshalling log line: %v", err)
		}

		select {
		case <-ctx.Done():
			return nil
		default:
			config.Logs <- &configLine
		}
	}

	return nil
}

// Capabilities returns the capabilities of the logger.
func (l *S3Logger) Capabilities() *logger.Capabilities {
	return &logger.Capabilities{ReadLogs: false, ReadConfig: false}
}
