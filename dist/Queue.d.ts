import Redis from 'ioredis';
import { RedisCredentials } from './interfaces/redis';
export default class Queue<DataType extends Record<string, string>> {
    private credentials;
    private pool;
    private processMap;
    private nConsumersMap;
    private activeConsumers;
    constructor(credentials: RedisCredentials);
    getClient(): Promise<Redis>;
    releaseClient(client: Redis): void;
    initStream(streamName: string, group: string): Promise<void>;
    private launchConsumerWithIdleTimeout;
    add(streamName: string, group: string, data: DataType): Promise<string | null>;
    process(streamName: string, nConsumers: number, callback: (message: DataType) => Promise<void>): Promise<void>;
}
