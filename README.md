# uploader-s3-object-storage
This code containse two parts:
producer and consumer

## Producer
this part get all files from a specific directory and set their path in queue in redis.

## Consumer
each consumer reads from the queue of redis and store them in their own queue and then upload them to s3 object storage. This part is multi threaded and number of the threads are configurable.


## Variables

### Producer
this variables are configurable in producer docker-compose file:

```
OSS_PATH: its the path on your local that you wanted to be uploade to s3.
QUEUE_NAME: name of queue of redis that path of files are stored to.
```

### Consumer
this variables are configurable in consumer docker-compose file:

```
QUEUE_NAME: the queue in redis that reads from. this should be the same as QUEUE_NAME in producer.
BUFFER_NAME: buffer name in redis that each consumer read from QUEUE_NAME and store it in this buffer.
BUFFER_COUNT: how many items each consumer read from QUEUE_NAME and store in its buffer.
SECRET_ACCESS_KEY: secret key of your s3
ACCESS_KEY_ID: access key of your s3
ENDPOINT_URL: endpoint of your s3
BUCKET_NAME: the name of the bucket you want to upload to.
PROCESS_COUNT: how many threads should run 
OBJECT_QUEUE: the name of queue in redis that each consumer store the name of the files that successfully uploaded to s3.
```

## Scale Consumer
the consumer is designed to scale in case of needed. for scale you can use docker compose scale command or add how many consumers as a service in docker-compose file



