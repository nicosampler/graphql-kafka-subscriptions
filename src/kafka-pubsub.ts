import * as Kafka from 'node-rdkafka';
import { PubSubEngine } from 'graphql-subscriptions';
import * as Logger from 'bunyan';
import { createChildLogger } from './child-logger';
import { PubSubAsyncIterator } from './pubsub-async-iterator';

export interface IKafkaOptions {
  topics: string[];
  host: string;
  logger?: Logger;
  groupId?: any;
}

export interface IKafkaProducer {
  write: (input: Buffer) => any;
}

export interface IKafkaTopic {
  readStream: any;
  writeStream: any;
}

const defaultLogger = Logger.createLogger({
  name: 'pubsub',
  stream: process.stdout,
  level: 'info'
});

export class KafkaPubSub implements PubSubEngine {
  private producers: { [key: string]: any };
  private subscriptionIndex: number;
  private options: any;
  private subscriptionMap: {
    [subId: number]: {
      topic: string;
      onMessageCb: (message: any) => any;
    };
  };
  private subscriptionsByTopic: { [topic: string]: Array<number> };
  private logger: Logger;

  constructor(options: IKafkaOptions) {
    this.options = options;
    this.subscriptionMap = {};
    this.subscriptionsByTopic = {};
    this.producers = {};
    this.logger = createChildLogger(
      this.options.logger || defaultLogger,
      'KafkaPubSub'
    );
    this.subscriptionIndex = 0;

    this.createConsumer(this.options.topics);
  }

  public asyncIterator<T>(triggers: string | string[]): AsyncIterator<T> {
    return new PubSubAsyncIterator<T>(this, triggers);
  }

  public subscribe(
    topic: string,
    onMessageCb: (message: any) => any
  ): Promise<number> {
    const index = this.subscriptionIndex;
    this.subscriptionIndex++;

    this.subscriptionMap[index] = { topic, onMessageCb };

    this.subscriptionsByTopic[topic] = [
      ...(this.subscriptionsByTopic[topic] || []),
      index
    ];

    return Promise.resolve(index);
  }

  public unsubscribe(index: number) {
    const { topic } = this.subscriptionMap[index];
    this.subscriptionsByTopic[topic] = this.subscriptionsByTopic[topic].filter(
      current => current !== index
    );
  }

  public publish(topic: string, message: any) {
    const producer = this.producers[topic] || this.createProducer(topic);
    return producer.write(new Buffer(JSON.stringify(message)));
  }

  private onMessage(topic: string, message) {
    const subscriptions = this.subscriptionsByTopic[topic];
    if (!subscriptions) {
      return;
    }

    for (const index of subscriptions) {
      const { onMessageCb } = this.subscriptionMap[index];
      onMessageCb(message);
    }
  }

  private createProducer(topic: string) {
    const kafkaProducer: {
      createWriteStream: (conf: any, topicConf: any, streamOptions: any) => any;
    } = Kafka.Producer as any;

    const producer = kafkaProducer.createWriteStream(
      {
        'metadata.broker.list': this.options.host
      },
      {},
      { topic }
    );
    producer.on('error', err => {
      this.logger.error(err, 'Error in our kafka stream');
    });
    this.producers[topic] = producer;
    return producer;
  }

  private createConsumer = (topics: [string]) => {
    const kafkaConsumer: {
      createReadStream: (conf: any, topicConf: any, streamOptions: any) => any;
    } = Kafka.KafkaConsumer as any;

    const groupId = this.options.groupId || Math.ceil(Math.random() * 9999);
    const consumer = kafkaConsumer.createReadStream(
      {
        'group.id': `kafka-group-${groupId}`,
        'metadata.broker.list': this.options.host
      },
      {},
      {
        topics
      }
    );
    consumer.on('data', message => {
      console.log(message);
      let parsedMessage = JSON.parse(message.value.toString());
      this.onMessage(message.topic, parsedMessage);
    });
    consumer.on('error', err => {
      this.logger.error(err, 'Error in our kafka stream');
    });
  };
}
