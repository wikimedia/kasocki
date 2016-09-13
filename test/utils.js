'use strict';


// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var assert = require('assert');

var utils = require('../lib/utils.js');

const topicsInfo = [
    { name: 'test0', partitions: [
        { id: 0, leader: 0, replicas: [ 0 ], isrs: [0] }
    ] },
    { name: 'test1', partitions: [
        { id: 0, leader: 0, replicas: [ 0 ], isrs: [0] },
        { id: 1, leader: 0, replicas: [ 0 ], isrs: [0] }
    ] },
];

describe('getAvailableTopics', () => {
    it('should return existent topics if allowedTopics is not specified', () => {
        let availableTopics = utils.getAvailableTopics(topicsInfo);
        assert.deepEqual(
            availableTopics,
            ['test0', 'test1']
        );
    });

    it('should return intersection of allowedTopics and existent topics', () => {
        let allowedTopics = ['test1'];
        let availableTopics = utils.getAvailableTopics(topicsInfo, allowedTopics);
        assert.deepEqual(
            availableTopics,
            allowedTopics
        );
    });
});


describe('buildPartitionAssignments', () => {
    it('should return empty array if topic does not exist', () => {
        let assignments = utils.buildPartitionAssignments(topicsInfo, ['does-not-exist']);
        assert.deepEqual(
            assignments,
            []
        );
    });

    it('should return assignments for a single partition topic', () => {
        let assignments = utils.buildPartitionAssignments(topicsInfo, ['test0']);
        assert.deepEqual(
            assignments,
            [ { topic: 'test0', partition: 0, offset: -1 } ]
        );
    });

    it('should return assignments for a multiple partition topic', () => {
        let assignments = utils.buildPartitionAssignments(topicsInfo, ['test1']);
        assert.deepEqual(
            assignments,
            [
                { topic: 'test1', partition: 0, offset: -1 },
                { topic: 'test1', partition: 1, offset: -1 }
            ]
        );
    });

    it('should return assignments for a multiple topics', () => {
        let assignments = utils.buildPartitionAssignments(topicsInfo, ['test0', 'test1']);
        assert.deepEqual(
            assignments,
            [
                { topic: 'test0', partition: 0, offset: -1 },
                { topic: 'test1', partition: 0, offset: -1 },
                { topic: 'test1', partition: 1, offset: -1 }
            ]
        );
    });
});

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
