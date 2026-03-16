package com.uplus.batch.domain.extraction.scheduler;

import java.time.LocalDateTime; 
import java.util.List;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.uplus.batch.domain.extraction.entity.EventStatus;
import com.uplus.batch.domain.extraction.entity.ExcellentEventStatus;
import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.repository.ExcellentEventStatusRepository;
import com.uplus.batch.domain.extraction.repository.ResultEventStatusRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class IntegratedRetryScheduler {

    private final ResultEventStatusRepository resultEventRepository;
    private final ExcellentEventStatusRepository excellentEventRepository;

    /**
     * 통합 복구 엔진: 실패 데이터 재시도 및 멈춰있는(좀비) 데이터 구출
     */
    @Scheduled(fixedDelay = 3600000) // 1시간마다 실행
    @Transactional
    public void retryAndRecoverTasks() {
        log.info("[Recovery] 통합 복구 엔진 가동...");

        // 1. [좀비 데이터 구출] 30분 이상 PROCESSING 상태로 멈춘 건들 복구 🚀
        LocalDateTime threshold = LocalDateTime.now().minusMinutes(30);
        int cleanedResult = resultEventRepository.cleanupStaleProcessingTasks(threshold);
        int cleanedScoring = excellentEventRepository.cleanupStaleProcessingTasks(threshold);
        
        if (cleanedResult > 0 || cleanedScoring > 0) {
            log.info("[Recovery] 좀비 데이터 복구 완료 - 요약: {}건, 채점: {}건", cleanedResult, cleanedScoring);
        }

        // 2. [실패 데이터 복구] 요약 실패 리트라이
        List<ResultEventStatus> failedSummaries = 
                resultEventRepository.findByStatusAndRetryCountLessThan(EventStatus.FAILED, 3);
        failedSummaries.forEach(ResultEventStatus::retry);

        // 3. [실패 데이터 복구] 채점 실패 리트라이
        List<ExcellentEventStatus> failedScoring = 
                excellentEventRepository.findByStatusAndRetryCountLessThan(EventStatus.FAILED, 3);
        failedScoring.forEach(ExcellentEventStatus::retry);

        log.info("[Recovery] 복구 프로세스 완료 (요약 실패: {}건, 채점 실패: {}건)", 
                failedSummaries.size(), failedScoring.size());
    }
}