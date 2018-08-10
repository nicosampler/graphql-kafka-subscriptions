"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var Kafka = require("node-rdkafka");
var Logger = require("bunyan");
var child_logger_1 = require("./child-logger");
var pubsub_async_iterator_1 = require("./pubsub-async-iterator");
var defaultLogger = Logger.createLogger({
    name: 'pubsub',
    stream: process.stdout,
    level: 'info'
});
var KafkaPubSub = (function () {
    function KafkaPubSub(options) {
        var _this = this;
        this.createConsumer = function (topics) {
            var kafkaConsumer = Kafka.KafkaConsumer;
            var groupId = _this.options.groupId || Math.ceil(Math.random() * 9999);
            var consumer = kafkaConsumer.createReadStream({
                'group.id': "kafka-group-" + groupId,
                'metadata.broker.list': _this.options.host
            }, {}, {
                topics: topics
            });
            consumer.on('data', function (message) {
                var parsedMessage = JSON.parse(message.value.toString());
                _this.onMessage(message.topic, parsedMessage);
            });
            consumer.on('error', function (err) {
                _this.logger.error(err, 'Error in our kafka stream');
            });
        };
        this.options = options;
        this.subscriptionMap = {};
        this.subscriptionsByTopic = {};
        this.producers = {};
        this.logger = child_logger_1.createChildLogger(this.options.logger || defaultLogger, 'KafkaPubSub');
        this.subscriptionIndex = 0;
        this.createConsumer(this.options.topics);
    }
    KafkaPubSub.prototype.asyncIterator = function (triggers) {
        return new pubsub_async_iterator_1.PubSubAsyncIterator(this, triggers);
    };
    KafkaPubSub.prototype.subscribe = function (topic, onMessageCb) {
        var index = this.subscriptionIndex;
        this.subscriptionIndex++;
        this.subscriptionMap[index] = { topic: topic, onMessageCb: onMessageCb };
        this.subscriptionsByTopic[topic] = (this.subscriptionsByTopic[topic] || []).concat([
            index
        ]);
        return Promise.resolve(index);
    };
    KafkaPubSub.prototype.unsubscribe = function (index) {
        var topic = this.subscriptionMap[index].topic;
        this.subscriptionsByTopic[topic] = this.subscriptionsByTopic[topic].filter(function (current) { return current !== index; });
    };
    KafkaPubSub.prototype.publish = function (topic, message) {
        var producer = this.producers[topic] || this.createProducer(topic);
        return producer.write(new Buffer(JSON.stringify(message)));
    };
    KafkaPubSub.prototype.onMessage = function (topic, message) {
        var subscriptions = this.subscriptionsByTopic[topic];
        if (!subscriptions) {
            return;
        }
        for (var _i = 0, subscriptions_1 = subscriptions; _i < subscriptions_1.length; _i++) {
            var index = subscriptions_1[_i];
            var onMessageCb = this.subscriptionMap[index].onMessageCb;
            onMessageCb(message);
        }
    };
    KafkaPubSub.prototype.createProducer = function (topic) {
        var _this = this;
        var kafkaProducer = Kafka.Producer;
        var producer = kafkaProducer.createWriteStream({
            'metadata.broker.list': this.options.host
        }, {}, { topic: topic });
        producer.on('error', function (err) {
            _this.logger.error(err, 'Error in our kafka stream');
        });
        this.producers[topic] = producer;
        return producer;
    };
    return KafkaPubSub;
}());
exports.KafkaPubSub = KafkaPubSub;
//# sourceMappingURL=kafka-pubsub.js.map