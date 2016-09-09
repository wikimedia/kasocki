'use strict';


// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var assert = require('assert');

var utils = require('../lib/utils.js');

describe('deserializeKafkaMessage', () => {
    it('should return an augmented message from a Kafka message', function() {
        let kafkaMessage = {
            message: '{ "first_name": "Dorkus", "last_name": "Berry" }',
            topic: 'test',
            partition: 1,
            offset: 123,
            key: 'myKey',
        };

        let msg = utils.deserializeKafkaMessage(kafkaMessage);
        assert.equal(msg._kafka.topic, kafkaMessage.topic, 'built message should have topic');
        assert.equal(msg._kafka.partition, kafkaMessage.partition, 'built message should have partition');
        assert.equal(msg._kafka.offset, kafkaMessage.offset, 'built message should have offset');
        assert.equal(msg._kafka.key, kafkaMessage.key, 'built message should have key');
    });
});



//  TODO: other utils.js unit tests
