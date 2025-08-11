package com.webbee.auditlistener.repository;

import com.webbee.auditlistener.model.AuditEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Репозиторий для работы с сущностями.
 * @author Evseeva Tsvetolina
 */
@Repository
public interface AuditEventRepository extends JpaRepository<AuditEvent, Long> {

}
