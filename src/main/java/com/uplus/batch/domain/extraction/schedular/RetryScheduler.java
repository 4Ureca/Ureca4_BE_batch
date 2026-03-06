package com.uplus.batch.domain.extraction.schedular;


import java.util.List;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.uplus.batch.domain.extraction.entity.EventStatus;
import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.repository.EventStatusRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class RetryScheduler {

    private final EventStatusRepository eventRepository;

    @Scheduled(fixedDelay = 3600000) 
    @Transactional 
    public void retryFailedTasks() {
        try {
            log.info("[Retry] 실패 데이터 재배치 엔진 가동...");

            List<ResultEventStatus> failedTasks = 
                eventRepository.findByStatusAndRetryCountLessThan(EventStatus.FAILED, 3);

            if (failedTasks.isEmpty()) {
                return;
            }

            for (ResultEventStatus task : failedTasks) {
                try {
                    task.retry();
                    log.debug("[Retry Success] ID: {}", task.getConsultId());
                } catch (Exception e) {
                    log.error("[Retry Task Error] 상담 ID {} 처리 중 오류: {}", 
                              task.getConsultId(), e.getMessage());
                }
            }
            
            eventRepository.saveAll(failedTasks);
            log.info("[Retry] 총 {}건의 데이터 재배치 완료", failedTasks.size());

        } catch (Exception e) {
            log.error("[Critical Retry Error] 재배치 스케줄러 실행 중 시스템 오류 발생: {}", e.getMessage());
        }
    }
}