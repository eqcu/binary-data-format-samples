// websocket-service/binary_websocket.ts
import * as msgpack from 'msgpack-lite';
import { WebSocket } from 'ws';

interface BinaryWebSocketOptions {
    formatType?: 'messagepack' | 'protobuf' | 'json';
    fallbackJson?: boolean;
    maxFrameSize?: number;
}

interface SerializedMessage {
    data: Buffer | string;
    isBinary: boolean;
}

export class BinaryWebSocketServer {
    private formatType: string;
    private fallbackJson: boolean;
    private maxFrameSize: number;

    constructor(options: BinaryWebSocketOptions = {}) {
        this.formatType = options.formatType || 'messagepack';
        this.fallbackJson = options.fallbackJson !== false;
        this.maxFrameSize = options.maxFrameSize || 1048576; // 1MB default
    }
    
    send<T>(ws: WebSocket, data: T): void {
        try {
            const serialized = this.serialize(data);
            if (serialized.isBinary) {
                ws.send(serialized.data, { binary: true });
            } else {
                ws.send(serialized.data);
            }
        } catch (error) {
            if (this.fallbackJson) {
                ws.send(JSON.stringify(data));
            } else {
                throw error;
            }
        }
    }
    
    onMessage<T>(ws: WebSocket, message: Buffer | string): T | null {
        try {
            if (Buffer.isBuffer(message)) {
                if (message.length > this.maxFrameSize) {
                    throw new Error(`Message size ${message.length} exceeds maximum ${this.maxFrameSize}`);
                }
                return msgpack.decode(message) as T;
            } else {
                return JSON.parse(message.toString()) as T;
            }
        } catch (error) {
            console.error('Failed to decode message:', error);
            return null;
        }
    }

    private serialize<T>(data: T): SerializedMessage {
        if (this.formatType === 'messagepack') {
            const binaryData = msgpack.encode(data);
            return { data: binaryData, isBinary: true };
        } else {
            return { data: JSON.stringify(data), isBinary: false };
        }
    }
}
