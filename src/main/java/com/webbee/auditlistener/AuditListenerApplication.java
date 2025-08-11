package com.webbee.auditlistener;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@EnableKafka
@EnableTransactionManagement
@SpringBootApplication
public class AuditListenerApplication {

    public static void main(String[] args) {
        SpringApplication.run(AuditListenerApplication.class, args);
    }

}
