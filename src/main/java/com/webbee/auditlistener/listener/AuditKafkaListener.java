package com.webbee.auditlistener.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webbee.auditlistener.model.AuditEvent;
import com.webbee.auditlistener.model.HttpRequestEvent;
import com.webbee.auditlistener.service.AuditEventService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

/**
 * Kafka listener для обработки событий аудита и HTTP-запросов.
 * @author Evseeva Tsvetolina
 */
@Component
public class AuditKafkaListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditKafkaListener.class);

    private final AuditEventService auditEventService;
    private final ObjectMapper objectMapper;

    private final List<DateTimeFormatter> timestampFormatters = Arrays.asList(
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
        DateTimeFormatter.ISO_LOCAL_DATE_TIME
    );

    public AuditKafkaListener(AuditEventService auditEventService, ObjectMapper objectMapper) {
        this.auditEventService = auditEventService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
        topics = "${kafka.topics.http-requests}",
        groupId = "${kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional("transactionManager")
    public void listenHttpRequestEvents(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        try {
            LOGGER.info("Получено HTTP событие из топика: {}, partition: {}, offset: {}", topic, partition, offset);

            HttpRequestEvent event = objectMapper.readValue(message, HttpRequestEvent.class);
            auditEventService.saveHttpRequestEvent(event);
            acknowledgment.acknowledge();
            LOGGER.info("HTTP событие успешно сохранено с ID: {}", event.getId());
        } catch (Exception e) {
            throw new RuntimeException("Не удалось обработать HTTP событие", e);
        }
    }

    @KafkaListener(
        topics = "${kafka.topics.audit-events}",
        groupId = "${kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional("transactionManager")
    public void listenAuditEvents(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        try {
            LOGGER.info("Получено Audit событие из топика: {}, partition: {}, offset: {}", topic, partition, offset);
            AuditEvent event = parseAuditEvent(message);
            auditEventService.saveAuditEvent(event);
            acknowledgment.acknowledge();
            LOGGER.info("Audit событие успешно сохранено с ID: {}", event.getId());
        } catch (Exception e) {
            throw new RuntimeException("Не удалось обработать Audit событие", e);
        }
    }

    private AuditEvent parseAuditEvent(String message) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(message);
        AuditEvent event = new AuditEvent();

        if (jsonNode.has("timestamp") && !jsonNode.get("timestamp").isNull()) {
            String timestampStr = jsonNode.get("timestamp").asText();
            LocalDateTime timestamp = parseTimestamp(timestampStr);
            event.setTimestamp(timestamp);
        }
        if (jsonNode.has("logLevel") && !jsonNode.get("logLevel").isNull()) {
            event.setLogLevel(jsonNode.get("logLevel").asText());
        }
        if (jsonNode.has("eventType") && !jsonNode.get("eventType").isNull()) {
            event.setEventType(jsonNode.get("eventType").asText());
        }
        if (jsonNode.has("correlationId") && !jsonNode.get("correlationId").isNull()) {
            event.setCorrelationId(jsonNode.get("correlationId").asText());
        }
        if (jsonNode.has("methodName") && !jsonNode.get("methodName").isNull()) {
            event.setMethodName(jsonNode.get("methodName").asText());
        }
        if (jsonNode.has("errorMessage") && !jsonNode.get("errorMessage").isNull()) {
            event.setErrorMessage(jsonNode.get("errorMessage").asText());
        }

        if (jsonNode.has("arguments") && !jsonNode.get("arguments").isNull()) {
            JsonNode argumentsNode = jsonNode.get("arguments");
            if (argumentsNode.isTextual()) {
                event.setArguments(argumentsNode.asText());
            } else {
                event.setArguments(objectMapper.writeValueAsString(argumentsNode));
            }
        }

        if (jsonNode.has("result") && !jsonNode.get("result").isNull()) {
            JsonNode resultNode = jsonNode.get("result");
            if (resultNode.isTextual()) {
                event.setResult(resultNode.asText());
            } else {
                event.setResult(objectMapper.writeValueAsString(resultNode));
            }
        }

        return event;
    }

    private LocalDateTime parseTimestamp(String timestampStr) {

        for (DateTimeFormatter formatter : timestampFormatters) {
            try {
                LocalDateTime result = LocalDateTime.parse(timestampStr, formatter);
                return result;
            } catch (Exception e) {
                throw new RuntimeException("Ошибка парсинга");
            }
        }
        return LocalDateTime.now();
    }

}
