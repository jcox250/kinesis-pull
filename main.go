package main

import (
	"context"
	"flag"
	"fmt"
	stdlog "log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/jcox250/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

var (
	millisBehindLatest = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kinesis_millis_behind_latest",
		Help: "Shows how many milliseconds behind the latest record",
	},
		[]string{"shard"},
	)

	writtenAt = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kinesis_written_at",
		Help: "Shows when the last record read from the shard was written",
	},
		[]string{"shard"},
	)
)

var (
	streamName        string
	roleArn           string
	shardIteratorType string
	timestamp         string
	awsRegion         string
	kinesisEndpoint   string
	debug             bool
	metricsPort       int
)

func init() {
	flag.StringVar(&streamName, "stream-name", "", "name of the kinesis stream")
	flag.StringVar(&roleArn, "role-arn", "", "role to assume")
	flag.StringVar(&shardIteratorType, "shard-iterator-type", "", "shard iterator type e.g TRIM_HORIZON")
	flag.StringVar(&timestamp, "timestamp", "", "timestamp to pass if you use AT_TIMESTAMP for the shard-iterator-type")
	flag.StringVar(&awsRegion, "aws-region", "", "aws region e.g us-east-1")
	flag.StringVar(&kinesisEndpoint, "kinesis-endpoint", "", "kinesis endpoint - used for local dev with localstack")
	flag.BoolVar(&debug, "debug", false, "enables debug logs")
	flag.IntVar(&metricsPort, "metrics-port", 9000, "port that metrics server uses")
	flag.Parse()

	prometheus.MustRegister(millisBehindLatest)
	prometheus.MustRegister(writtenAt)
}

func main() {
	logger := log.NewLeveledLogger(os.Stderr, debug)
	logger.Info("msg", "kinesis config", "stream-name", streamName, "role-arn", roleArn, "shard-iterator-type", shardIteratorType, "timestamp", timestamp, "aws-region", awsRegion)
	logger.Info("msg", "service config", "debug", debug, "metricsPort", metricsPort)

	kc, err := NewKinesisClient(KinesisConfig{
		StreamName:        streamName,
		RoleArn:           roleArn,
		ShardIteratorType: shardIteratorType,
		AWSRegion:         awsRegion,
		Endpoint:          kinesisEndpoint,
		Timestamp:         timestamp,
		Logger:            logger,
	})
	if err != nil {
		stdlog.Fatal(err)
	}

	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(fmt.Sprintf(":%d", metricsPort), nil)

	ctx, cancel := context.WithCancel(context.Background())
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)
	go func() {
		<-sigc
		cancel()
	}()

	kc.Sub(ctx)
}

type KinesisConfig struct {
	StreamName        string
	RoleArn           string
	ShardIteratorType string
	AWSRegion         string
	Endpoint          string
	Shards            []string
	Timestamp         string
	Logger            log.LeveledLogger
}

type KinesisClient struct {
	logger            log.LeveledLogger
	client            *kinesis.Kinesis
	streamName        string
	shards            []string
	shardIteratorType string
	timestamp         string
}

func NewKinesisClient(k KinesisConfig) (*KinesisClient, error) {
	s := session.Must(session.NewSession())
	config := &aws.Config{
		Region:   aws.String(k.AWSRegion),
		Endpoint: aws.String(k.Endpoint),
	}

	if k.RoleArn != "" {
		config.Credentials = stscreds.NewCredentials(s, k.RoleArn)
	}

	kc := &KinesisClient{
		logger:            k.Logger,
		client:            kinesis.New(s, config),
		streamName:        k.StreamName,
		shards:            k.Shards,
		shardIteratorType: k.ShardIteratorType,
		timestamp:         k.Timestamp,
	}

	if len(kc.shards) == 0 {
		shards, err := kc.getShards()
		if err != nil {
			return nil, err
		}
		kc.shards = shards
	}

	return kc, nil
}

func (k *KinesisClient) Sub(ctx context.Context) {
	k.logger.Debug("msg", "reading from kinesis", "shards", fmt.Sprintf("%v", k.shards))
	wg := sync.WaitGroup{}
	for _, shard := range k.shards {
		wg.Add(1)
		go func(sid string) {
			defer wg.Done()
			if err := k.readShard(ctx, sid); err != nil {
				k.logger.Error("msg", "failed to read shard", "shardID", sid, "err", err)
			}
		}(shard)
	}

	k.logger.Debug("msg", "waiting for readShard goroutines to finish")
	wg.Wait()
}

func (k *KinesisClient) readShard(ctx context.Context, shardID string) error {
	iterator, err := k.getShardIterator(shardID)
	if err != nil {
		return err
	}

	for {
		out, err := k.client.GetRecordsWithContext(ctx, &kinesis.GetRecordsInput{
			ShardIterator: aws.String(iterator),
		})
		if err != nil {
			if err == context.Canceled {
				k.logger.Info("msg", "context canceled")
				return nil
			}
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				// If throttled then wait and try again
				case kinesis.ErrCodeProvisionedThroughputExceededException, kinesis.ErrCodeKMSThrottlingException:
					time.Sleep(300 * time.Millisecond)
					continue
				}
			}
			k.logger.Error("msg", "failed to get records for shard")
			return err
		}

		if len(out.Records) > 0 {
			for _, record := range out.Records {
				fmt.Fprintln(os.Stdout, string(record.Data))
				if record.ApproximateArrivalTimestamp != nil {
					writtenAt.WithLabelValues(shardID).Set(float64(record.ApproximateArrivalTimestamp.Unix()))
				}
			}

		}

		if out.MillisBehindLatest != nil {
			millisBehindLatest.WithLabelValues(shardID).Set(float64(*out.MillisBehindLatest))
		}

		if out.NextShardIterator == nil {
			k.logger.Error("msg", "shard closed")
			return nil
		}
		iterator = *out.NextShardIterator
	}
}

func (k *KinesisClient) getShards() ([]string, error) {
	res, err := k.client.ListShards(&kinesis.ListShardsInput{
		StreamName: aws.String(k.streamName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list shards: %s", err)
	}

	shards := []string{}
	for _, s := range res.Shards {
		shards = append(shards, *s.ShardId)
	}
	return shards, nil
}

func (k *KinesisClient) getShardIterator(shardID string) (string, error) {
	var timestamp *time.Time
	if k.timestamp != "" && k.shardIteratorType == kinesis.ShardIteratorTypeAtTimestamp {
		t, err := time.Parse(time.RFC3339, k.timestamp)
		if err != nil {
			return "", fmt.Errorf("failed to parse timestamp: %s", err)
		}
		timestamp = &t
	}
	input := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardID),
		ShardIteratorType: aws.String(k.shardIteratorType),
		StreamName:        aws.String(k.streamName),
		Timestamp:         timestamp,
	}

	out, err := k.client.GetShardIterator(input)
	if err != nil {
		return "", err
	}

	return *out.ShardIterator, nil
}
