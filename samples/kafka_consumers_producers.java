// kafka-services/src/main/java/MessagePackSerializer.java
public class MessagePackSerializer implements Serializer<Object> {
    private final ObjectMapper msgpackMapper = new ObjectMapper(new MessagePackFactory());
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final boolean fallbackToJson;
    
    @Override
    public byte[] serialize(String topic, Object data) {
        try {
            return msgpackMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            if (fallbackToJson) {
                return jsonMapper.writeValueAsBytes(data);
            }
            throw new SerializationException("MessagePack serialization failed", e);
        }
    }
}

// Kafka Configuration
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessagePackSerializer.class.getName());
props.put("fallback.json.enabled", "true");
props.put("serialization.format", "messagepack");
