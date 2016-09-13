'use strict';

/**
 * Collection of utility functions for Kasocki.
 */

const objectutils         = require('./objectutils');

const errors              = require('./error.js');
const InvalidMessageError = errors.InvalidMessageError;

const kafka               = require('node-rdkafka');
const P                   = require('bluebird');


/**
 * Returns a Promise of a promisified and connected rdkafka.KafkaConsumer.
 *
 * @param  {Object} kafkaConfig
 * @param  {Object} topicConfig
 * @return {Promise<KafkaConsumer>} Promisified KafkaConsumer
 */
function createKafkaConsumerAsync(kafkaConfig, topicConfig) {
    topicConfig = topicConfig || {};

    const consumer = P.promisifyAll(
        new kafka.KafkaConsumer(kafkaConfig, topicConfig)
    );

    return consumer.connectAsync(undefined)
    .then((metadata) => {
        return consumer;
    });
}


/**
 * Return the intersection of existent topics and allowedTopics,
 * or just all existent topics if allowedTopics is undefined.
 *
 * @param  {Array}  topicsInfo topic metadata object, _metadata.topics.
 * @param  {Array}  allowedTopics topics allowed to be consumed.
 * @return {Array}  available topics
 */
function getAvailableTopics(topicsInfo, allowedTopics) {
    const existentTopics = topicsInfo.map(
        e => e.name
    )
    .filter(t => t !== '__consumer_offsets');

    if (allowedTopics) {
        return existentTopics.filter(
            t => allowedTopics.indexOf(t) >= 0
        );
    }
    else {
        return existentTopics;
    }
}


/**
 * Given an Array of topics, this will return an array of
 * [{topic: t1, partition: 0, offset: -1}, ...]
 * for each topic-partition.  This is useful for manually passing
 * an to KafkaConsumer.assign, without actually subscribing
 * a consumer group with Kafka.
 *
 * @param  {Array}  topicsInfo topic metadata object, _metadata.topics.
 * @param  {Array}  topics we want to build partition assignments for.
 * @return {Array}  TopicPartition assignments starting at latest offset.
 */
function buildPartitionAssignments(topicsInfo, topics) {
    // Find the topic metadata we want
    return topicsInfo.filter((t) => {
        return topics.indexOf(t.name) >= 0;
    })
    // Map them into topic, partition, offset: -1 (latest) assignment.
    .map((t) => {
        return t.partitions.map((p) => {
            return { topic: t.name, partition: p.id, offset: -1 };
        });
    })
    // Flatten, returning empty array if nothing found
    .reduce((a, b) => a.concat(b), []);
}


/**
 * Parses kafkaMessage.message as a JSON string and then
 * augments the object with kafka message metadata.
 * in the _kafka sub object.
 *
 * @param  {KafkaMesssage} kafkaMessage
 * @return {Object}
 *
 */
function deserializeKafkaMessage(kafkaMessage) {
    let message = objectutils.factory(kafkaMessage.message);

    message._kafka = {
        topic:     kafkaMessage.topic,
        partition: kafkaMessage.partition,
        offset:    kafkaMessage.offset,
        key:       kafkaMessage.key
    };
    return message;
}


module.exports = {
    createKafkaConsumerAsync:       createKafkaConsumerAsync,
    getAvailableTopics:             getAvailableTopics,
    buildPartitionAssignments:      buildPartitionAssignments,
    deserializeKafkaMessage:        deserializeKafkaMessage,
};
