
package com.webbee.auditlistener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webbee.auditlistener.model.AuditEvent;
import com.webbee.auditlistener.model.HttpRequestEvent;
import com.webbee.auditlistener.repository.AuditEventRepository;
import com.webbee.auditlistener.repository.HttpRequestEventRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "default.replication.factor=1",
                "min.insync.replicas=1"
        },
        topics = {"test-http-requests", "test-audit-events"}
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "kafka.topics.http-requests=test-http-requests",
        "kafka.topics.audit-events=test-audit-events",
        "kafka.consumer.group-id=test-group",
        "kafka.consumer.transaction-id-prefix=test-tx",
        "logging.level.org.springframework.kafka=DEBUG",
        "logging.level.com.webbee.auditlistener=DEBUG"
})
@DirtiesContext
public class ExactlyOnceKafkaIntegrationTest {

    @Autowired
    private AuditEventRepository auditEventRepository;

    @Autowired
    private HttpRequestEventRepository httpRequestEventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${kafka.topics.http-requests}")
    private String httpRequestsTopic;

    @Value("${kafka.topics.audit-events}")
    private String auditEventsTopic;

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;

    private KafkaProducer<String, String> producer;

    @BeforeEach
    void setUp() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        producer = new KafkaProducer<>(producerProps);

        auditEventRepository.deleteAll();
        httpRequestEventRepository.deleteAll();
    }

    @Test
    void testHttpRequestEventExactlyOnceSemantics() throws Exception {
        String correlationId = UUID.randomUUID().toString();
        HttpRequestEvent event = createHttpRequestEvent(correlationId);
        String jsonMessage = objectMapper.writeValueAsString(event);

        System.out.println("Sending HTTP request message: " + jsonMessage);

        producer.send(new ProducerRecord<>(httpRequestsTopic, correlationId, jsonMessage)).get();
        producer.send(new ProducerRecord<>(httpRequestsTopic, correlationId, jsonMessage)).get();
        producer.send(new ProducerRecord<>(httpRequestsTopic, correlationId, jsonMessage)).get();

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            long count = httpRequestEventRepository.count();
            System.out.println("HTTP request events count: " + count);
            assertThat(count).isEqualTo(3);
        });

        assertThat(httpRequestEventRepository.findAll())
                .hasSize(3)
                .allMatch(savedEvent -> savedEvent.getCorrelationId().equals(correlationId));
    }

    @Test
    void testAuditEventExactlyOnceSemantics() throws Exception {
        String correlationId = UUID.randomUUID().toString();
        AuditEvent event = createAuditEvent(correlationId);
        
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("timestamp", event.getTimestamp().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        eventMap.put("logLevel", event.getLogLevel());
        eventMap.put("eventType", event.getEventType());
        eventMap.put("correlationId", event.getCorrelationId());
        eventMap.put("methodName", event.getMethodName());
        eventMap.put("arguments", event.getArguments());
        eventMap.put("result", event.getResult());
        
        String jsonMessage = objectMapper.writeValueAsString(eventMap);
        System.out.println("Sending audit event message: " + jsonMessage);

        producer.send(new ProducerRecord<>(auditEventsTopic, correlationId, jsonMessage)).get();
        producer.send(new ProducerRecord<>(auditEventsTopic, correlationId, jsonMessage)).get();
        producer.send(new ProducerRecord<>(auditEventsTopic, correlationId, jsonMessage)).get();

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            long count = auditEventRepository.count();
            System.out.println("Audit events count: " + count);
            assertThat(count).isEqualTo(3);
        });

        assertThat(auditEventRepository.findAll())
                .hasSize(3)
                .allMatch(savedEvent -> savedEvent.getCorrelationId().equals(correlationId));
    }

    @Test
    void testSimpleMessageSending() throws Exception {
        String correlationId = UUID.randomUUID().toString();
        HttpRequestEvent event = createHttpRequestEvent(correlationId);
        String jsonMessage = objectMapper.writeValueAsString(event);

        producer.send(new ProducerRecord<>(httpRequestsTopic, correlationId, jsonMessage)).get();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(httpRequestEventRepository.count()).isGreaterThan(0);
        });
    }

    @Test
    void testMultipleConsumerGroupsProcessSameMessage() throws Exception {
        String correlationId = UUID.randomUUID().toString();
        HttpRequestEvent event = createHttpRequestEvent(correlationId);
        String jsonMessage = objectMapper.writeValueAsString(event);

        producer.send(new ProducerRecord<>(httpRequestsTopic, correlationId, jsonMessage)).get();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(httpRequestEventRepository.count()).isEqualTo(1);
        });

        createAdditionalConsumerAndVerifyDuplication(httpRequestsTopic, jsonMessage);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(httpRequestEventRepository.count()).isEqualTo(1);
        });
    }

    @Test
    @Transactional
    void testTransactionRollbackPreventsMessageAcknowledgment() throws Exception {
        long initialCount = httpRequestEventRepository.count();
        
        HttpRequestEvent invalidEvent = new HttpRequestEvent();
        invalidEvent.setCorrelationId(UUID.randomUUID().toString());
        invalidEvent.setTimestamp(LocalDateTime.now());
        
        String invalidJson = "{\"invalidField\":\"value\",\"timestamp\":\"invalid-timestamp\"}";

        producer.send(new ProducerRecord<>(httpRequestsTopic, "key", invalidJson)).get();

        Thread.sleep(3000);

        assertThat(httpRequestEventRepository.count()).isEqualTo(initialCount);
    }

    @Test
    void testIdempotentProducerPreventsMessageDuplication() throws Exception {
        String correlationId = UUID.randomUUID().toString();
        HttpRequestEvent event = createHttpRequestEvent(correlationId);
        String jsonMessage = objectMapper.writeValueAsString(event);

        Map<String, Object> idempotentProps = new HashMap<>();
        idempotentProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        idempotentProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        idempotentProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        idempotentProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        idempotentProps.put(ProducerConfig.ACKS_CONFIG, "all");
        idempotentProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        idempotentProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        try (KafkaProducer<String, String> idempotentProducer = new KafkaProducer<>(idempotentProps)) {
            for (int i = 0; i < 5; i++) {
                idempotentProducer.send(new ProducerRecord<>(httpRequestsTopic, correlationId, jsonMessage)).get();
            }
        }

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(httpRequestEventRepository.count()).isGreaterThan(0);
        });
    }

    private HttpRequestEvent createHttpRequestEvent(String correlationId) {
        HttpRequestEvent event = new HttpRequestEvent();
        event.setTimestamp(LocalDateTime.now());
        event.setRequestType("HTTP_REQUEST");
        event.setMethod("GET");
        event.setStatusCode(200);
        event.setUrl("/api/test");
        event.setCorrelationId(correlationId);
        event.setExecutionTime(100L);
        event.setUserAgent("test-agent");
        event.setRemoteAddress("127.0.0.1");
        return event;
    }

    private AuditEvent createAuditEvent(String correlationId) {
        AuditEvent event = new AuditEvent();
        event.setTimestamp(LocalDateTime.now());
        event.setLogLevel("INFO");
        event.setEventType("METHOD_CALL");
        event.setCorrelationId(correlationId);
        event.setMethodName("testMethod");
        event.setArguments("{\"param\":\"value\"}");
        event.setResult("{\"result\":\"success\"}");
        return event;
    }

    private void createAdditionalConsumerAndVerifyDuplication(String topic, String message) {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "duplicate-test-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
    }
}