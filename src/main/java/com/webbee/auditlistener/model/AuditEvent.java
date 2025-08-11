package com.webbee.auditlistener.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Сущность для представления событий аудита в системе.
 * @author Evseeva Tsvetolina
 */
@Entity
@Table(name = "audit_events")
@Data
public class AuditEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    @Column(name = "timestamp", nullable = false)
    private LocalDateTime timestamp;

    @Column(name = "log_level", length = 20)
    private String logLevel;

    @Column(name = "event_type", length = 100)
    private String eventType;

    @Column(name = "correlation_id", length = 100)
    private String correlationId;

    @Column(name = "method_name", length = 500)
    private String methodName;

    @Lob
    @Column(name = "arguments")
    private String arguments;

    @Lob
    @Column(name = "result")
    private String result;

    @Lob
    @Column(name = "error_message")
    private String errorMessage;

}
