# redis-client/binary_redis.py
import redis
import msgpack

class BinaryRedisClient:
    def __init__(self, **kwargs):
        self.client = redis.Redis(decode_responses=False, **kwargs)
        self.codec = BinaryCodec(format_type='messagepack')
    
    def set(self, key, value, **kwargs):
        binary_data = self.codec.encode(value)
        return self.client.set(key, binary_data, **kwargs)
    
    def get(self, key):
        binary_data = self.client.get(key)
        if binary_data:
            return self.codec.decode(binary_data)
        return None
    
    def pipeline_binary_ops(self, operations):
        pipe = self.client.pipeline()
        for op_type, key, value in operations:
            if op_type == 'set':
                pipe.set(key, self.codec.encode(value))
            elif op_type == 'get':
                pipe.get(key)
        results = pipe.execute()
        return [self.codec.decode(r) if r else None for r in results]
