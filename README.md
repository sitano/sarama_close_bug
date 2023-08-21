# sarama_close_bug

consumer group deadlock on close: consumer group shutdown hangs #1351 test case

https://github.com/IBM/sarama/issues/1351

# What is it

`close` method of consumer group without partition never finishes.

# How to run it

```
$ ... run kafka locally with docker with image like https://hub.docker.com/r/wurstmeister/kafka/
$ ... exposing 9092 port to the local host

$ env GO111MODULE=on go build -race && ./sarama_close_bug -brokers 127.0.0.1:9092

2019/06/05 15:01:47 Starting a new Sarama consumer
2019/06/05 15:01:47 client connected
2019/06/05 15:01:47 new topic with single partition =  5a5fb18d-343e-4bf8-9dea-886729a6b28e
2019/06/05 15:01:47 new group =  196cff9d-c66b-4666-a76a-0d93047500e7
2019/06/05 15:01:47 starting  1
2019/06/05 15:01:47 starting  2
2019/06/05 15:01:47 consume at  1
2019/06/05 15:01:47 consume at  2
2019/06/05 15:01:48 consumer setup 1
2019/06/05 15:01:48 consumer cleanup 1
2019/06/05 15:01:48 consume at  1
2019/06/05 15:01:48 consumer setup 2
2019/06/05 15:01:48 consumer setup 1
2019/06/05 15:01:48 Sarama consumers up and running!...
2019/06/05 15:01:48 trying to close cs... 2
^C
```

and it never finishes...
