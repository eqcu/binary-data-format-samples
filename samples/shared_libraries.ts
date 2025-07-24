// shared-libs/serialization/binary-codec.ts
import * as msgpack from 'msgpack-lite';

export type SerializationFormat = 'messagepack' | 'protobuf' | 'json';

export interface BinaryCodecOptions {
    formatType?: SerializationFormat;
    fallbackJson?: boolean;
    maxSize?: number;
}

export interface SerializationMetrics {
    originalSize: number;
    compressedSize: number;
    compressionRatio: number;
    serializationTimeMs: number;
}

export class BinaryCodec {
    private formatType: SerializationFormat;
    private fallbackJson: boolean;
    private maxSize: number;

    constructor(options: BinaryCodecOptions = {}) {
        this.formatType = options.formatType || 'messagepack';
        this.fallbackJson = options.fallbackJson !== false;
        this.maxSize = options.maxSize || 10 * 1024 * 1024; // 10MB default
    }
    
    async encode<T>(data: T): Promise<{ buffer: Buffer; metrics: SerializationMetrics }> {
        const startTime = Date.now();
        const originalSize = Buffer.byteLength(JSON.stringify(data));
        
        try {
            let buffer: Buffer;
            
            if (this.formatType === 'messagepack') {
                buffer = msgpack.encode(data);
            } else if (this.formatType === 'protobuf') {
                buffer = await this.encodeProtobuf(data);
            } else {
                buffer = Buffer.from(JSON.stringify(data), 'utf-8');
            }
            
            if (buffer.length > this.maxSize) {
                throw new Error(`Encoded size ${buffer.length} exceeds maximum ${this.maxSize}`);
            }
            
            const metrics: SerializationMetrics = {
                originalSize,
                compressedSize: buffer.length,
                compressionRatio: originalSize / buffer.length,
                serializationTimeMs: Date.now() - startTime
            };
            
            return { buffer, metrics };
        } catch (error) {
            if (this.fallbackJson) {
                const buffer = Buffer.from(JSON.stringify(data), 'utf-8');
                const metrics: SerializationMetrics = {
                    originalSize,
                    compressedSize: buffer.length,
                    compressionRatio: 1,
                    serializationTimeMs: Date.now() - startTime
                };
                return { buffer, metrics };
            }
            throw error;
        }
    }
            
    async decode<T>(buffer: Buffer, schema?: any): Promise<T> {
        try {
            if (this.formatType === 'messagepack') {
                return msgpack.decode(buffer) as T;
            } else if (this.formatType === 'protobuf') {
                return await this.decodeProtobuf<T>(buffer, schema);
            } else {
                return JSON.parse(buffer.toString('utf-8')) as T;
            }
        } catch (error) {
            if (this.fallbackJson) {
                return JSON.parse(buffer.toString('utf-8')) as T;
            }
            throw error;
        }
    }

    private async encodeProtobuf<T>(data: T): Promise<Buffer> {
        // Protobuf encoding implementation
        throw new Error('Protobuf encoding not yet implemented');
    }

    private async decodeProtobuf<T>(buffer: Buffer, schema?: any): Promise<T> {
        // Protobuf decoding implementation
        throw new Error('Protobuf decoding not yet implemented');
    }
}
