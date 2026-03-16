package com.uplus.batch.domain.extraction.scheduler;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.repository.ExcellentEventStatusRepository;
import com.uplus.batch.domain.extraction.repository.ResultEventStatusRepository;
import com.uplus.batch.domain.extraction.service.ConsultationAnalysisManager;
import com.uplus.batch.domain.extraction.service.ConsultationAnalysisManager.TaskPair;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class IntegratedAnalysisScheduler {

    private final ResultEventStatusRepository resultEventRepository;
    private final ExcellentEventStatusRepository excellentEventRepository;
    private final ConsultationAnalysisManager analysisManager;

    // 중복 실행 방지를 위한 원자적 플래그
    private final AtomicBoolean isProcessing = new AtomicBoolean(false);

    @Scheduled(fixedDelay = 60000)
    public void executeIntegratedAnalysis() {
        // [STEP 1] 문지기 체크: 원자적으로 false일 때만 true로 바꾸고 통과 
        if (!isProcessing.compareAndSet(false, true)) {
            log.warn("[Batch] 이전 통합 분석 작업이 아직 진행 중입니다. 이번 스케줄은 건너뜁니다.");
            return;
        }

        log.info("[Batch] 통합 분석 엔진 가동 - 분석 대기 데이터 확인 중...");

        try {
            // [STEP 2] 데이터 조회 
            List<ResultEventStatus> pendingResults = 
                resultEventRepository.findReadyToProcessPairs(PageRequest.of(0, 50));

            if (pendingResults.isEmpty()) {
                log.info("[Batch] 처리할 데이터가 없습니다.");
                return;
            }

            log.info("[Batch] 데이터 준비 완료 - {}건 (번들 분석 시작)", pendingResults.size());

            // [STEP 3] 50개를 TaskPair 리스트로 변환 (짝 맞추기)
            List<TaskPair> taskPairs = pendingResults.stream()
                .map(resultTask -> {
                    return excellentEventRepository.findByConsultId(resultTask.getConsultId())
                        .map(excellentTask -> new TaskPair(resultTask, excellentTask))
                        .orElse(null);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

            if (taskPairs.isEmpty()) {
                log.warn("[Batch] 짝이 맞는 태스크가 없어 종료합니다.");
                return;
            }

            // [STEP 4] 매니저에게 번들 처리 위임
            analysisManager.processIntegratedBundledTasks(taskPairs);

            log.info("[Batch] 통합 번들 분석 프로세스 완료 - {}건 처리됨", taskPairs.size());

        } catch (Exception e) {
            log.error("[Critical Error] 통합 번들 처리 중 예상치 못한 오류: {}", e.getMessage(), e);
        } finally {
            // [STEP 5] 작업 종료 후 플래그 해제 
            isProcessing.set(false);
        }
    }
}