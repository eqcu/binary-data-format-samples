// microservices/order-service/src/app.ts
import express from 'express';
import { BinaryCodec } from '../../shared-libs/serialization/binary-codec';
import { KafkaBinaryClient } from '../../kafka-services/src/kafka-binary-client';
import { BinaryRedisClient } from '../../redis-client/binary-redis';
import { BinaryWebSocketServer } from '../../websocket-service/binary_websocket';

interface OrderEvent {
    orderId: string;
    customerId: string;
    items: OrderItem[];
    totalAmount: number;
    timestamp: string;
}

interface OrderItem {
    productId: string;
    quantity: number;
    price: number;
}

export class OrderService {
    private kafka: KafkaBinaryClient;
    private redis: BinaryRedisClient;
    private wsServer: BinaryWebSocketServer;
    private codec: BinaryCodec;

    constructor() {
        const serializationFormat = (process.env.SERIALIZATION_FORMAT as any) || 'messagepack';
        const fallbackEnabled = process.env.ENABLE_BINARY_FALLBACK === 'true';

        this.kafka = new KafkaBinaryClient({
            clientId: 'order-service',
            brokers: process.env.KAFKA_BROKERS?.split(',') || ['localhost:9092'],
            serializationFormat,
            fallbackJson: fallbackEnabled
        });

        this.redis = new BinaryRedisClient({
            host: process.env.REDIS_HOST || 'localhost',
            port: parseInt(process.env.REDIS_PORT || '6379'),
            formatType: serializationFormat,
            fallbackJson: fallbackEnabled
        });

        this.wsServer = new BinaryWebSocketServer({
            formatType: serializationFormat,
            fallbackJson: fallbackEnabled,
            maxFrameSize: parseInt(process.env.WS_MAX_FRAME_SIZE || '1048576')
        });

        this.codec = new BinaryCodec({
            formatType: serializationFormat,
            fallbackJson: fallbackEnabled
        });
    }

    async initialize(): Promise<void> {
        await this.kafka.connect();
        
        // Subscribe to order events
        await this.kafka.subscribe<OrderEvent>(['order-created', 'order-updated'], 
            this.handleOrderEvent.bind(this)
        );
    }

    async createOrder(orderData: Omit<OrderEvent, 'orderId' | 'timestamp'>): Promise<string> {
        const orderId = `order-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        const orderEvent: OrderEvent = {
            orderId,
            ...orderData,
            timestamp: new Date().toISOString()
        };

        try {
            // Store in Redis cache
            await this.redis.set(`order:${orderId}`, orderEvent, { EX: 3600 }); // 1 hour TTL

            // Publish to Kafka
            await this.kafka.sendMessage('order-created', {
                key: orderId,
                value: orderEvent,
                headers: {
                    'event-type': 'order-created',
                    'service': 'order-service',
                    'version': '1.0'
                }
            });

            return orderId;
        } catch (error) {
            console.error('Failed to create order:', error);
            throw new Error('Order creation failed');
        }
    }

    async getOrder(orderId: string): Promise<OrderEvent | null> {
        try {
            // Try Redis cache first
            let order = await this.redis.get(`order:${orderId}`);
            
            if (!order) {
                // Fallback to database query
                order = await this.queryOrderFromDatabase(orderId);
                if (order) {
                    // Cache for future requests
                    await this.redis.set(`order:${orderId}`, order, { EX: 3600 });
                }
            }

            return order as OrderEvent | null;
        } catch (error) {
            console.error('Failed to retrieve order:', error);
            throw new Error('Order retrieval failed');
        }
    }

    private async handleOrderEvent(topic: string, orderEvent: OrderEvent, headers?: Record<string, string>): Promise<void> {
        console.log(`Processing ${topic} event for order ${orderEvent.orderId}`);
        
        try {
            // Update cache
            await this.redis.set(`order:${orderEvent.orderId}`, orderEvent, { EX: 3600 });
            
            // Notify WebSocket clients
            this.notifyWebSocketClients(orderEvent);
            
            // Process business logic based on event type
            if (topic === 'order-created') {
                await this.processNewOrder(orderEvent);
            } else if (topic === 'order-updated') {
                await this.processOrderUpdate(orderEvent);
            }
        } catch (error) {
            console.error(`Failed to handle ${topic} event:`, error);
            // Implement dead letter queue or retry logic
        }
    }

    private async processNewOrder(order: OrderEvent): Promise<void> {
        // Business logic for new orders
        console.log(`Processing new order: ${order.orderId}`);
    }

    private async processOrderUpdate(order: OrderEvent): Promise<void> {
        // Business logic for order updates
        console.log(`Processing order update: ${order.orderId}`);
    }

    private notifyWebSocketClients(orderEvent: OrderEvent): void {
        // WebSocket notification logic
        console.log(`Notifying WebSocket clients about order ${orderEvent.orderId}`);
    }

    private async queryOrderFromDatabase(orderId: string): Promise<OrderEvent | null> {
        // Database query implementation
        console.log(`Querying database for order ${orderId}`);
        return null; // Placeholder
    }

    async shutdown(): Promise<void> {
        await this.kafka.disconnect();
        console.log('Order service shutdown complete');
    }
}

// Express server setup with binary format support
const app = express();
const orderService = new OrderService();

app.use(express.json());

app.get('/health', (req, res) => {
    res.json({ status: 'healthy', serialization: process.env.SERIALIZATION_FORMAT });
});

app.post('/orders', async (req, res) => {
    try {
        const orderId = await orderService.createOrder(req.body);
        res.status(201).json({ orderId });
    } catch (error) {
        res.status(500).json({ error: 'Failed to create order' });
    }
});

app.get('/orders/:orderId', async (req, res) => {
    try {
        const order = await orderService.getOrder(req.params.orderId);
        if (order) {
            res.json(order);
        } else {
            res.status(404).json({ error: 'Order not found' });
        }
    } catch (error) {
        res.status(500).json({ error: 'Failed to retrieve order' });
    }
});

// Graceful shutdown
process.on('SIGTERM', async () => {
    console.log('Received SIGTERM, shutting down gracefully');
    await orderService.shutdown();
    process.exit(0);
});

// Initialize and start server
orderService.initialize().then(() => {
    const port = process.env.PORT || 3000;
    app.listen(port, () => {
        console.log(`Order service listening on port ${port}`);
        console.log(`Using serialization format: ${process.env.SERIALIZATION_FORMAT || 'messagepack'}`);
    });
}).catch(error => {
    console.error('Failed to initialize order service:', error);
    process.exit(1);
});
