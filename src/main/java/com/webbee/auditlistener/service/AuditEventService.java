package com.webbee.auditlistener.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webbee.auditlistener.model.AuditEvent;
import com.webbee.auditlistener.model.HttpRequestEvent;
import com.webbee.auditlistener.repository.AuditEventRepository;
import com.webbee.auditlistener.repository.HttpRequestEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Сервис для управления событиями аудита.
 * @author Evseeva Tsvetolina
 */
@Service
@Transactional("transactionManager")
public class AuditEventService {

    private final HttpRequestEventRepository httpRequestEventRepository;
    private final AuditEventRepository auditEventRepository;
    private final ObjectMapper objectMapper;

    public AuditEventService(HttpRequestEventRepository httpRequestEventRepository,
                           AuditEventRepository auditEventRepository,
                           ObjectMapper objectMapper) {
        this.httpRequestEventRepository = httpRequestEventRepository;
        this.auditEventRepository = auditEventRepository;
        this.objectMapper = objectMapper;
    }

    public HttpRequestEvent saveHttpRequestEvent(HttpRequestEvent event) {
        return httpRequestEventRepository.save(event);
    }

    public AuditEvent saveAuditEvent(AuditEvent event) {
        return auditEventRepository.save(event);
    }

}
