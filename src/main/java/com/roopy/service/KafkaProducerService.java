package com.roopy.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {

    private static int runningId = 0;

    private final KafkaTemplate kafkaTemplate;

    @Scheduled(fixedRate = 1000*10, initialDelay = 5*1000)
    public void produceMessage() {
        log.info("Produce Message - BEGIN");
        String message = String.format("%d 번째 메세지를 %s 에 전송 하였습니다.", runningId++, LocalDateTime.now().toString());
        ListenableFuture<SendResult<String, String>> listenableFuture = kafkaTemplate.send("testTopic", message);
        listenableFuture.addCallback(new ListenableFutureCallback<Object>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("ERROR Kafka error happend", ex);
            }

            @Override
            public void onSuccess(Object result) {
                log.info("SUCCESS!! This is the reulst: {}", result);
            }
        });

        log.info("Produce Message - END {}", message);
    }

}
