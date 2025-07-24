# shared-libs/serialization/binary_codec.py
class BinaryCodec:
    def __init__(self, format_type='messagepack', fallback_json=True):
        self.format_type = format_type
        self.fallback_json = fallback_json
        
    def encode(self, data):
        try:
            if self.format_type == 'messagepack':
                return msgpack.packb(data, default=self._msgpack_encoder)
            elif self.format_type == 'protobuf':
                return self._encode_protobuf(data)
        except Exception as e:
            if self.fallback_json:
                return json.dumps(data).encode('utf-8')
            raise
            
    def decode(self, binary_data, schema=None):
        try:
            if self.format_type == 'messagepack':
                return msgpack.unpackb(binary_data, raw=False)
            elif self.format_type == 'protobuf':
                return self._decode_protobuf(binary_data, schema)
        except Exception as e:
            if self.fallback_json:
                return json.loads(binary_data.decode('utf-8'))
            raise
