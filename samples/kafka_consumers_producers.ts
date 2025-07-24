// kafka-services/src/kafka-binary-client.ts
import { Kafka, Producer, Consumer, EachMessagePayload } from 'kafkajs';
import { BinaryCodec, SerializationFormat } from '../shared-libs/serialization/binary-codec';

export interface KafkaBinaryConfig {
    clientId: string;
    brokers: string[];
    serializationFormat?: SerializationFormat;
    fallbackJson?: boolean;
}

export interface BinaryMessage<T = any> {
    key?: string;
    value: T;
    headers?: Record<string, string>;
    timestamp?: string;
}

export class KafkaBinaryClient {
    private kafka: Kafka;
    private producer: Producer;
    private consumer: Consumer;
    private codec: BinaryCodec;

    constructor(config: KafkaBinaryConfig) {
        this.kafka = new Kafka({
            clientId: config.clientId,
            brokers: config.brokers,
        });
        
        this.producer = this.kafka.producer();
        this.consumer = this.kafka.consumer({ groupId: `${config.clientId}-consumer` });
        
        this.codec = new BinaryCodec({
            formatType: config.serializationFormat || 'messagepack',
            fallbackJson: config.fallbackJson !== false
        });
    }

    async connect(): Promise<void> {
        await Promise.all([
            this.producer.connect(),
            this.consumer.connect()
        ]);
    }

    async disconnect(): Promise<void> {
        await Promise.all([
            this.producer.disconnect(),
            this.consumer.disconnect()
        ]);
    }

    async sendMessage<T>(topic: string, message: BinaryMessage<T>): Promise<void> {
        try {
            const { buffer, metrics } = await this.codec.encode(message.value);
            
            await this.producer.send({
                topic,
                messages: [{
                    key: message.key,
                    value: buffer,
                    headers: {
                        ...message.headers,
                        'content-encoding': this.codec.formatType,
                        'original-size': metrics.originalSize.toString(),
                        'compressed-size': metrics.compressedSize.toString(),
                        'serialization-time': metrics.serializationTimeMs.toString()
                    },
                    timestamp: message.timestamp
                }]
            });
            
            // Emit metrics for monitoring
            this.emitSerializationMetrics('kafka-producer', metrics);
        } catch (error) {
            console.error('Failed to send binary message:', error);
            throw error;
        }
    }

    async subscribe<T>(
        topics: string[], 
        messageHandler: (topic: string, message: T, headers?: Record<string, string>) => Promise<void>
    ): Promise<void> {
        await this.consumer.subscribe({ topics });
        
        await this.consumer.run({
            eachMessage: async ({ topic, message, partition }: EachMessagePayload) => {
                try {
                    if (!message.value) return;
                    
                    const decodedValue = await this.codec.decode<T>(message.value);
                    const headers = this.parseHeaders(message.headers);
                    
                    await messageHandler(topic, decodedValue, headers);
                } catch (error) {
                    console.error(`Failed to process message from topic ${topic}:`, error);
                    // Implement dead letter queue or retry logic here
                }
            }
        });
    }

    private parseHeaders(headers: any): Record<string, string> {
        const result: Record<string, string> = {};
        if (headers) {
            Object.keys(headers).forEach(key => {
                result[key] = headers[key]?.toString() || '';
            });
        }
        return result;
    }

    private emitSerializationMetrics(component: string, metrics: SerializationMetrics): void {
        // Integration with metrics collection (Prometheus, etc.)
        console.log(`${component} serialization metrics:`, metrics);
    }
}
