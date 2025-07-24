// redis-client/binary-redis.ts
import Redis, { RedisOptions } from 'ioredis';
import { BinaryCodec, SerializationFormat } from '../shared-libs/serialization/binary-codec';

export interface BinaryRedisOptions extends RedisOptions {
    formatType?: SerializationFormat;
    fallbackJson?: boolean;
    pipelineSize?: number;
}

export interface RedisSetOptions {
    EX?: number; // Expire time in seconds
    PX?: number; // Expire time in milliseconds
    NX?: boolean; // Only set if key doesn't exist
    XX?: boolean; // Only set if key exists
}

export class BinaryRedisClient {
    private client: Redis;
    private codec: BinaryCodec;
    private pipelineSize: number;

    constructor(options: BinaryRedisOptions = {}) {
        const { formatType, fallbackJson, pipelineSize, ...redisOptions } = options;
        
        this.client = new Redis({
            ...redisOptions,
            lazyConnect: true,
            keepAlive: 30000,
            retryDelayOnFailover: 100,
            maxRetriesPerRequest: 3
        });
        
        this.codec = new BinaryCodec({
            formatType: formatType || 'messagepack',
            fallbackJson: fallbackJson !== false
        });
        
        this.pipelineSize = pipelineSize || 100;
    }

    async connect(): Promise<void> {
        await this.client.connect();
    }

    async disconnect(): Promise<void> {
        await this.client.disconnect();
    }

    async set<T>(key: string, value: T, options?: RedisSetOptions): Promise<'OK' | null> {
        try {
            const { buffer } = await this.codec.encode(value);
            
            if (options) {
                const args: any[] = [];
                if (options.EX) args.push('EX', options.EX);
                if (options.PX) args.push('PX', options.PX);
                if (options.NX) args.push('NX');
                if (options.XX) args.push('XX');
                
                return await this.client.set(key, buffer, ...args);
            } else {
                return await this.client.set(key, buffer);
            }
        } catch (error) {
            console.error(`Failed to set key ${key}:`, error);
            throw error;
        }
    }

    async get<T>(key: string): Promise<T | null> {
        try {
            const buffer = await this.client.getBuffer(key);
            if (buffer) {
                return await this.codec.decode<T>(buffer);
            }
            return null;
        } catch (error) {
            console.error(`Failed to get key ${key}:`, error);
            throw error;
        }
    }

    async mget<T>(...keys: string[]): Promise<(T | null)[]> {
        try {
            const buffers = await this.client.mgetBuffer(...keys);
            const results: (T | null)[] = [];
            
            for (const buffer of buffers) {
                if (buffer) {
                    results.push(await this.codec.decode<T>(buffer));
                } else {
                    results.push(null);
                }
            }
            
            return results;
        } catch (error) {
            console.error('Failed to mget keys:', error);
            throw error;
        }
    }

    async del(...keys: string[]): Promise<number> {
        return await this.client.del(...keys);
    }

    async exists(...keys: string[]): Promise<number> {
        return await this.client.exists(...keys);
    }

    async pipelineBinaryOps<T>(operations: Array<{
        type: 'set' | 'get' | 'del';
        key: string;
        value?: T;
        options?: RedisSetOptions;
    }>): Promise<any[]> {
        const pipeline = this.client.pipeline();
        
        for (const op of operations) {
            switch (op.type) {
                case 'set':
                    if (op.value !== undefined) {
                        const { buffer } = await this.codec.encode(op.value);
                        if (op.options) {
                            const args: any[] = [op.key, buffer];
                            if (op.options.EX) args.push('EX', op.options.EX);
                            if (op.options.PX) args.push('PX', op.options.PX);
                            if (op.options.NX) args.push('NX');
                            if (op.options.XX) args.push('XX');
                            pipeline.set(...args);
                        } else {
                            pipeline.set(op.key, buffer);
                        }
                    }
                    break;
                case 'get':
                    pipeline.getBuffer(op.key);
                    break;
                case 'del':
                    pipeline.del(op.key);
                    break;
            }
        }
        
        const results = await pipeline.exec();
        if (!results) return [];
        
        const decodedResults: any[] = [];
        let opIndex = 0;
        
        for (const [error, result] of results) {
            if (error) {
                decodedResults.push(error);
            } else {
                const op = operations[opIndex];
                if (op.type === 'get' && result) {
                    decodedResults.push(await this.codec.decode(result as Buffer));
                } else {
                    decodedResults.push(result);
                }
            }
            opIndex++;
        }
        
        return decodedResults;
    }

    // Health check method
    async ping(): Promise<string> {
        return await this.client.ping();
    }

    // Get Redis info
    async info(section?: string): Promise<string> {
        return await this.client.info(section);
    }
}
