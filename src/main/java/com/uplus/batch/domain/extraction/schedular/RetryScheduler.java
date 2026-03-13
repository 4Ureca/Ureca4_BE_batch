package com.uplus.batch.domain.extraction.schedular;


import java.util.List;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.uplus.batch.domain.extraction.entity.EventStatus;
import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.repository.EventStatusRepository;
import com.uplus.batch.domain.summary.entity.SummaryEventStatus;
import com.uplus.batch.domain.summary.repository.SummaryEventStatusRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class RetryScheduler {

    private final EventStatusRepository eventRepository;
    private final SummaryEventStatusRepository summaryEventStatusRepo;

    // fixedDelayString으로 변경하여 테스트 시 application.yml의 retry.fixed-delay로 제어 가능
    // 기본값 3600000ms (1시간), 테스트 시 99999999ms로 설정하여 자동 실행 방지
    @Scheduled(fixedDelayString = "${retry.fixed-delay:3600000}")
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

    /** SummaryEventStatus FAILED 건 재배치 (retryCount < 3) */
    @Scheduled(fixedDelayString = "${retry.fixed-delay:3600000}")
    @Transactional
    public void retryFailedSummaryTasks() {
        try {
            List<SummaryEventStatus> failedTasks =
                    summaryEventStatusRepo.findByStatusAndRetryCountLessThan(
                            com.uplus.batch.domain.summary.enums.SummaryEventStatusCode.FAILED.getValue(), 3);

            if (failedTasks.isEmpty()) return;

            for (SummaryEventStatus task : failedTasks) {
                task.retry();
            }
            summaryEventStatusRepo.saveAll(failedTasks);
            log.info("[Retry] SummaryEventStatus {}건 REQUESTED 재배치 완료", failedTasks.size());
        } catch (Exception e) {
            log.error("[Critical Retry Error] SummaryEventStatus 재배치 중 오류: {}", e.getMessage());
        }
    }
}