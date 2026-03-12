package com.uplus.batch.synthetic;

import com.uplus.batch.domain.extraction.entity.EventStatus;
import com.uplus.batch.domain.extraction.repository.EventStatusRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 5분 주기 합성 상담 데이터 생성 스케줄러.
 *
 * <p>트랜잭션 경계 (스펙 §5-4 준수):
 * <ol>
 *   <li>Step 1 — {@code factory.executeStep1()} @Transactional : 반환 시 커밋 완료</li>
 *   <li>Step 2 — {@code factory.triggerAiExtraction()} : Step 1 커밋 후 별도 INSERT</li>
 *   <li>Step 3 — {@code factory.triggerSummaryGeneration()} : Step 1 커밋 후 별도 INSERT</li>
 * </ol>
 *
 * <p>중복 실행 방지: {@link AtomicBoolean}으로 단일 JVM 내 동시 실행 차단.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SyntheticDataGeneratorScheduler {

    private final SyntheticConsultationFactory factory;
    private final SyntheticDataProperties properties;
    private final EventStatusRepository eventStatusRepository;
    private final FailureAlertService alertService;

    /** 동일 JVM 내 중복 실행 방지 플래그. 단일 인스턴스 배포 기준으로 충분. */
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public void generate() {
        if (!properties.isEnabled()) {
            log.debug("[SyntheticDataGenerator] 비활성화 (synthetic-data.enabled=false) — 스킵");
            return;
        }

        if (!isRunning.compareAndSet(false, true)) {
            log.warn("[SyntheticDataGenerator] 이전 실행이 완료되지 않아 이번 회차 스킵");
            return;
        }

        long startTime = System.currentTimeMillis();
        int  batchSize = properties.getBatchSize();

        try {
            log.info("[SyntheticDataGenerator] Step1 시작 - batchSize={}, targetDate={}",
                    batchSize, LocalDate.now());

            // ── Step 1: consultation_results + raw_texts ─────────────────────────────────
            // @Transactional 메서드를 외부 Bean에서 호출 → Spring AOP 프록시 적용, 커밋 보장
            SyntheticConsultationFactory.BatchResult step1 = factory.executeStep1(batchSize);

            if (step1.isEmpty()) {
                log.warn("[SyntheticDataGenerator] Step1 결과 없음 — Step2·3 스킵");
                return;
            }

            int actual = step1.consultIds().size();
            log.info("[SyntheticDataGenerator] Step1 완료 - inserted rawTexts={}, results={}", actual, actual);

            // ── Step 2: result_event_status REQUESTED (Step 1 커밋 후 별도 실행) ────────
            factory.triggerAiExtraction(step1.consultIds(), step1.categoryCodes());
            log.info("[SyntheticDataGenerator] Step2 트리거 완료 - ResultEventStatus {}건 REQUESTED", actual);

            // ── Step 3: summary_event_status REQUESTED (Step 1 커밋 후 별도 실행) ───────
            factory.triggerSummaryGeneration(step1.consultIds());
            log.info("[SyntheticDataGenerator] Step3 트리거 완료 - SummaryEventStatus {}건 REQUESTED", actual);

            checkAndAlertFailures();

        } catch (Exception e) {
            log.error("[SyntheticDataGenerator] 실행 중 오류 발생: {}", e.getMessage(), e);
        } finally {
            isRunning.set(false);
            log.info("[SyntheticDataGenerator] 전체 완료 - 소요시간={}ms",
                    System.currentTimeMillis() - startTime);
        }
    }

    /**
     * retryCount >= 3 인 FAILED 건이 failureAlertThreshold 이상이면 알람을 발송한다.
     * 기존 RetryScheduler는 retryCount < 3 인 건만 재배치하므로
     * retryCount >= 3 = 더 이상 자동 복구 불가능한 영구 실패를 의미한다.
     */
    private void checkAndAlertFailures() {
        try {
            int permanentlyFailed = eventStatusRepository
                    .countByStatusAndRetryCountGreaterThanEqual(EventStatus.FAILED, 3);

            if (permanentlyFailed >= properties.getFailureAlertThreshold()) {
                alertService.alert(
                        "[SyntheticDataGenerator] AI 요약 영구 실패 건 임계값 초과 (retryCount >= 3)",
                        permanentlyFailed
                );
            }
        } catch (Exception e) {
            log.warn("[SyntheticDataGenerator] 실패 임계값 체크 중 오류 (무시): {}", e.getMessage());
        }
    }
}
