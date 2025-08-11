package com.webbee.auditlistener.repository;

import com.webbee.auditlistener.model.HttpRequestEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface HttpRequestEventRepository extends JpaRepository<HttpRequestEvent, Long> {
}