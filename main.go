package main

import (
	"context"
	"flag"
	"fmt"
	stdlog "log"
	"os"
	"time"

	"github.com/jcox250/log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

var (
	streamName        string
	roleArn           string
	shardIteratorType string
	timestamp         string
	awsRegion         string
	debug             bool
)

func init() {
	flag.StringVar(&streamName, "stream-name", "", "name of the kinesis stream")
	flag.StringVar(&roleArn, "role-arn", "", "role to assume")
	flag.StringVar(&shardIteratorType, "shard-iterator-type", "", "shard iterator type e.g TRIM_HORIZON")
	flag.StringVar(&timestamp, "timestamp", "", "timestamp to pass if you use AT_TIMESTAMP for the shard-iterator-type")
	flag.StringVar(&awsRegion, "aws-region", "", "aws region e.g us-east-1")
	flag.BoolVar(&debug, "debug", false, "enables debug logs")
	flag.Parse()
}

func main() {
	//ctx := setupLogger(context.Background(), debug)
	logger := log.NewLeveledLogger(os.Stderr, debug)
	kc, err := NewKinesisClient(KinesisConfig{
		StreamName:        streamName,
		RoleArn:           roleArn,
		ShardIteratorType: shardIteratorType,
		AWSRegion:         awsRegion,
		Timestamp:         timestamp,
		Logger:            logger,
	})
	if err != nil {
		stdlog.Fatal(err)
	}

	ctx := context.Background()
	kc.Sub(ctx)
}

type KinesisConfig struct {
	StreamName        string
	RoleArn           string
	ShardIteratorType string
	AWSRegion         string
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
	creds := stscreds.NewCredentials(s, k.RoleArn)

	kc := &KinesisClient{
		logger:            k.Logger,
		client:            kinesis.New(s, &aws.Config{Region: aws.String(k.AWSRegion), Credentials: creds}),
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
	for _, shard := range k.shards {
		if err := k.readShard(ctx, shard); err != nil {
			k.logger.Error("msg", "failed to read shard", "shardID", shard, "err", err)
		}
	}
}

func (k *KinesisClient) readShard(ctx context.Context, shardID string) error {
	iterator, err := k.getShardIterator(shardID)
	if err != nil {
		return err
	}

	for {
		out, err := k.client.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: aws.String(iterator),
		})
		if err != nil {
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
				fmt.Println(string(record.Data))
				if record.ApproximateArrivalTimestamp != nil {
					fmt.Println("WrittenAt: ", *record.ApproximateArrivalTimestamp)
				}
				fmt.Println("")
			}

		}

		if out.MillisBehindLatest != nil {
			k.logger.Debug("MillisBehindLatest", *out.MillisBehindLatest)
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
