## zenskar-assignment
--

The application implements a POC where an event stream is generated and get stored in any s3 compatible storage

### producer

Producer produces a 1kb message every 10ms and inserts it into a redis stream called zenskar
The message size can be changed by changing the array size

### consumer
Consumer continuously keep listening to the stream. You can run multiple consumers as long as all the consumers are part of same consumer group. The consumer read the messages in a batch of 100 and upload to a s3 compatible storage in a multipart upload request.