# Kasocki

[![Travis](https://travis-ci.org/wikimedia/kasocki.svg?branch=master)](https://travis-ci.org/wikimedia/kasocki)
[![Coveralls](https://coveralls.io/repos/github/wikimedia/kasocki/badge.svg?branch=master)](https://coveralls.io/github/wikimedia/kasocki?branch=master)


Kafka Consumer -> socket.io library.  All messages in Kafka are assumed to be
utf-8 JSON strings.  These are decoded and augmented, and then emitted
to connected socket.io clients.

Supports topic subscription and assignment at partition offsets and server side field filtering.

When Kafka supports it, this will also support timestamp based consumption.

## Usage

### Set up Kasocki socket.io server

```javascript
const server = require('http').createServer();
const io = require('socket.io')(server);
const Kasocki = require('kasocki');

io.on('connection', (socket) => {
    // Create a new Kasocki instance bound to the socket.
    // The socket.io client can then subscribe to topics,
    // specify filters, and start consuming.
    let kasocki = new Kasocki(socket, {
        kafkaConfig: {'metadata.broker.list': 'mybroker:9092'}
    });
    kasocki.connect()
    .then(() => {
        console.log(`Kasocki ready for socket ${socket.name}`)
    });
});

server.listen(6927);
console.log('Listening for socket.io connections at localhost:6927');
```

### socket.io client set up

```javascript
var socket = require('socket.io-client')('localhost:6927');

// Log errors and responses from socket.io event callbacks.
function ackCallback(err, res) {
    if (err) {
        console.log('Got error: ', err);
    }
    else {
        console.log('ACK from server with', res);
    }
}


// Subscribe to some topics.
let topics = [
     // subscribe to mytopic1 and mytopic2 starting at latest offset in each
    'mytopic1',
    'mytopic2'
]
socket.emit('subscribe', topics, ackCallback);

// filter for messages based on fields, regexes supported.
let filters = {
    // message.top_level_field must === 1
    'top_level_field': 1,
    // AND message.sub.field must match this regex
    'sub.field': '/^(green|blue).*/'
}
socket.emit('filter', filters);


// Consume 3 messages, receiving them via ackCallback.
socket.emit('consume', null, ackCallback);
socket.emit('consume', null, ackCallback);
socket.emit('consume', null, ackCallback);
```


Consuming using `message` handler and `start`:
```javascript
// Register an on 'message' handler to receive messages
// after starting a continuous consumer.
socket.on('message', function(message){
    console.log('Received: ', message);
});

// start consuming as fast as we can.
socket.emit('start', null);

const BBPromise = require('bluebird');

// stop consuming after 2 seconds
BBPromise.delay(2000)
.then(() => {
    socket.emit('stop', null);
})
// resume consuming after 2 seconds
.delay(2000)
.then(() => {
    socket.emit('start', null);
}
// Disconnect after 3 seconds
.delay(3000)
.then(() => {
    socket.disconnect()
});

```

Subscribe at specified topic partition offsets:
```javascript

// Subscribe to some topics, specifying all partitions and
// offsets from which to start.
let topicAssignments = [
    { topic: 'mytopic1', partition: 0, offset: 5012 },
    { topic: 'mytopic1', partition: 1, offset: 5056 },
    { topic: 'mytopic2', partition: 0, offset: 1023 },
];
socket.emit('subscribe', topicAssignments, ackCallback);
```

Most socket events will return errors in the ack callback, but
you can also receive them via an `err` socket event handler.
This is especially useful for receiving and handling
errors that might happen during the async streaming consume loop
that runs after `start` is emitted.

```
socket.on('err', (e) => {
    consloe.log('Got error from Kasocki server', e);
});
```


## `consume` vs `start`

`consume` socket events are not disabled once `start` has been issued.
`start` simply runs the same logic that `consume` runs, except instead of
returning the message via the `ackCallback`, it emits a `message` event
and then consumes again in a loop.
If a client issues a `consume` while Kasocki has already been started,
then that consume call will consume a message from Kafka and return it
via the ackCallback, but that message _will not be_ emitted as a `message`
event, since it will have been consumed asynchronously outside of the
`start` consume loop.


## Notes on Kafka consumer state

In normal use cases, Kafka (and previously Zookeeper) handles consumer state.
Kafka keeps track of multiple consumer processes in named consumer groups, and
handles rebalancing of those processes as they come and go.  Kafka also
handles offset commits, keeping track of the high water mark each consumer
has reached in each topic and partition.

Kasocki is intended to be exposed to the public internet by enabling
web based consumers to use websockets to consume from Kafka.  Since
the internet at large cannot be trusted, we would prefer to avoid allowing
the internet to make any state changes to our Kafka clusters.  Kasocki
pushes as much consumer state management to the connected clients as it can.

Offset commits are not supported.  Instead, each message is augmented with
`topic`, `partition` and `offset` (and `key`) fields.  This information can be
used during subscription to specify the position at which Kasocki should start
consuming from Kafka.

Consumer group management is also not supported.  Each new socket.io client
corresponds to a new consumer group.  There is no way to parallelize
consumption from Kafka for a single connected client.  Ideally, we would not
register a consumer group at all with Kafka, but as of this writing
[librdkafka](https://github.com/Blizzard/node-rdkafka/issues/18) and
[blizzard/node-rdkafka](https://github.com/Blizzard/node-rdkafka/issues/18)
don't support this yet.  Consumer groups that are registered with Kafka
are named after the socket.io socket id, i.e. `kasocki-${socket.id}`.

For simplicity, Kasocki does not support unsubscription.
Oonce a socket.io client has succesfully issued a `subscribe` socket
event, further `subscribe` socket events will result in an error.
It may be possible to implement unsubscribe or re-subscribe, but until
this is necessary it is simpler to not allow it.  Re-subscription can
be done by disconnecting the socket.io client and creating and subscribing
a new one.


## Blizzard Consume Modes
The Blizzard Kafka client that Kasocki uses has
[several consume APIs](https://github.com/Blizzard/node-rdkafka#kafkakafkaconsumer).
Kasocki uses the [Standard Non flowing API](https://github.com/Blizzard/node-rdkafka#standard-api-1),
and socket.io clients can choose to receive messages either via the `consume`
socket event's ackCallback, or by emitting a `start` event, and listening for
on `message` events to be sent by Kasocki.


Quick testing of each mode:
- Non flowing with ackCb to socket io:     800/s
- Non flowing with on.data emit message:  6000/s  (this is enough for our use case)
- flowing mode with on.data emit: didn't work with lots of messages, but
  perhaps I was doing it wrong:

```
RangeError: Maximum call stack size exceeded
/home/otto/kasocki/node_modules/socket.io/node_modules/engine.io/lib/socket.js:413
      this.sentCallbackFn.push.apply(this.sentCallbackFn, this.packetsFn);
```

It is likely more efficient to use the flowing consume mode for clients
that want to receive `message` events as fast as possible.  Doing so
would make `consume` and `start` incompatible modes, and would also
change the way the `subscribe` event interface currently works.  We may
do this work later down the road.


## Testing
Mocha tests require a running 0.9+ Kafka broker at `localhost:9092` with
`delete.topic.enable=true`.  `test/utils/clean_kafka.sh` will prepare
topics in Kafka for tests.  `npm test` will run this script.

Note that there is a
[bug in librdkafka/node-rdkafka](https://github.com/edenhill/librdkafka/issues/775)
that keeps tests from shutting down once done.  This bug also has implications
for the number of consumers a process can run at once in its lifetime,
and will have to be resolved somehow before this is put into production.

## TODO

- Feedback to client if their assignment offset does not exist anymore
- tests for kafkaEventHandlers
- Filter for array values
- filter glob wildcards?
- filter with some other thing?  JSONPath?
- move Kafka test topic and data to fixtures?
- docker with kafka
- Get upstream fix for https://github.com/Blizzard/node-rdkafka/issues/5
  this will need to be resolved before this can be used in any type of production
  setting.
- rdkafka statsd: https://phabricator.wikimedia.org/T145099
