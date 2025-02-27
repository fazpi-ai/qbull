import { RedisOptions } from 'ioredis';

export interface IJobData {
    id: number;
    data: any;
    groupName: string;
    progress: (value: number) => Promise<void>;
}

export interface ICallback {
    (job: IJobData, done: (err?: Error) => void): void;
}

export interface IProcessEntry {
    callback: ICallback;
}

export interface IQueueInitOptions {
    credentials: RedisOptions;
    consumerLimits?: Record<string, number>;
    logLevel: 'silent' | 'debug';
}

export interface ILuaScriptResult {
    jobId: number;
    jobData: Record<string, any>; // O podrías usar `IJobData` si tienes una interfaz definida
    groupName: string;
}


export interface ILastTaskTime {
    [queueName: string]: Date;
}