package com.example.springkafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;

@Service
public class KafkaMessageProcessor {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ");

    private JsonNode messageA;
    private JsonNode messageB;

    public void processMessageFromTopicA(String message) {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            if (isValidMessage(jsonNode)) {
                this.messageA = jsonNode;
                joinAndSendMessages();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void processMessageFromTopicB(String message) {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            if (isValidMessage(jsonNode)) {
                this.messageB = jsonNode;
                joinAndSendMessages();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean isValidMessage(JsonNode message) {
        return message.has("catalog_number") && message.get("catalog_number").asText().length() == 5
                && message.has("country") && message.get("country").asText().equals("001")
                && isValidDate(message.path("value").path("selling_status_date").asText())
                && isValidDate(message.path("value").path("sales_date").asText());
    }

    private boolean isValidDate(String dateStr) {
        try {
            dateFormat.parse(dateStr);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    private void joinAndSendMessages() {
        if (messageA != null && messageB != null &&
                messageA.path("catalog_number").asText().equals(messageB.path("catalog_number").asText()) &&
                messageA.path("country").asText().equals(messageB.path("country").asText())) {

            // Create joined message
            JsonNode joinedMessage = objectMapper.createObjectNode()
                    .put("catalog_number", messageA.path("catalog_number").asText())
                    .put("country", messageA.path("country").asText())
                    .put("is_selling", messageA.path("value").path("is_selling").asBoolean())
                    .put("model", messageA.path("value").path("model").asText())
                    .put("product_id", messageA.path("value").path("product_id").asText())
                    .put("registration_id", messageA.path("value").path("registration_id").asText())
                    .put("registration_number", messageA.path("value").path("registration_number").asText())
                    .put("selling_status_date", messageA.path("value").path("selling_status_date").asText())
                    .put("order_number", messageB.path("value").path("order_number").asText())
                    .put("quantity", messageB.path("value").path("quantity").asInt())
                    .put("sales_date", messageB.path("value").path("sales_date").asText());

            kafkaProducerService.sendMessage("TOPIC_C", joinedMessage);

            // Reset messages after processing
            this.messageA = null;
            this.messageB = null;
        }
    }
}

