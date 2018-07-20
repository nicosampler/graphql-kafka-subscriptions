/// <reference types="node" />
import { PubSubEngine } from 'graphql-subscriptions';
import * as Logger from 'bunyan';
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
export declare class KafkaPubSub implements PubSubEngine {
    private producers;
    private subscriptionIndex;
    private options;
    private subscriptionMap;
    private subscriptionsByTopic;
    private logger;
    constructor(options: IKafkaOptions);
    asyncIterator<T>(triggers: string | string[]): AsyncIterator<T>;
    subscribe(topic: string, onMessageCb: (message: any) => any): Promise<number>;
    unsubscribe(index: number): void;
    publish(topic: string, message: any): any;
    private onMessage;
    private createProducer;
    private createConsumer;
}
