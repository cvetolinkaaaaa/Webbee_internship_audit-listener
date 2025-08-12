package com.webbee.auditlistener.repository;

import com.webbee.auditlistener.model.HttpRequestEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Репозиторий для работы с сущностями.
 * @author Evseeva Tsvetolina
 */
@Repository
public interface HttpRequestEventRepository extends JpaRepository<HttpRequestEvent, Long> {

}
