# Kinesis Pull

Kinesis pull is a tool for pulling data from a kinesis stream. 

```
Usage of ./kinesis-pull:
  -aws-region string
    	aws region e.g us-east-1
  -debug
    	enables debug logs
  -kinesis-endpoint string
    	kinesis endpoint - used for local dev with localstack
  -metrics-port int
    	port that metrics server uses (default 9000)
  -role-arn string
    	role to assume
  -shard-iterator-type string
    	shard iterator type e.g TRIM_HORIZON
  -stream-name string
    	name of the kinesis stream
  -timestamp string
    	timestamp to pass if you use AT_TIMESTAMP for the shard-iterator-type
```

## Getting Started

This example assumes you have a kinesis stream containing data running in localstack. If you don't have this you can set one up by following the README in [kinesis-push](https://github.com/jcox250/kinesis-push). If you have a kinesis stream running in AWS then you'll need to pass a valid `-aws-region` and `-role-arn` to access the stream.

```
$ ./kinesis-pull -debug -shard-iterator-type TRIM_HORIZON -stream-name test-stream -kinesis-endpoint http://localhost:4566 -aws-region foo | tee kinesis-data.txt
level=info msg="kinesis config" stream-name=test-stream role-arn= shard-iterator-type=TRIM_HORIZON timestamp= aws-region=foo
level=info msg="service config" debug=true metricsPort=9000
level=debug msg="reading from kinesis" shards="[shardId-000000000000 shardId-000000000001 shardId-000000000002 shardId-000000000003 shardId-000000000004]"
level=debug msg="waiting for readShard goroutines to finish"
{"name": "foo", "age": 21}
{"name": "bar", "age": 22}
{"name": "fizz", "age": 23}
{"name": "buzz", "age": 24}
{"name": "foobar", "age": 25}
```

You should now have a file with containing the data from kinesis
```
$ cat kinesis-data.txt
{"name": "foo", "age": 21}
{"name": "bar", "age": 22}
{"name": "fizz", "age": 23}
{"name": "buzz", "age": 24}
{"name": "foobar", "age": 25}
```

