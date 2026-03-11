package com.uplus.batch.historical;

import com.uplus.batch.synthetic.SyntheticConsultationFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 과거 상담 데이터 생성 배치 서비스.
 *
 * <h3>역할 분리</h3>
 * <pre>
 * [이 배치]
 *   consultation_results + raw_texts + 연관 테이블 생성 (created_at = targetDate)
 *   result_event_status  → REQUESTED
 *   summary_event_status → requested
 *
 * [ExtractionScheduler — 별도 배치]
 *   result_event_status=REQUESTED 감지
 *   → Gemini API 호출 → retention_analysis 저장
 *   → result_event_status=COMPLETED
 *
 * [SummarySyncItemWriter — 별도 배치]
 *   summary_event_status=requested + result_event_status=COMPLETED 감지
 *   → KeywordProcessor (ES 형태소 분석)
 *   → MongoDB consultation_summary upsert
 *   → ES 인덱싱
 *   → summary_event_status=completed
 * </pre>
 *
 * <h3>체크포인트</h3>
 * <ul>
 *   <li>날짜별 {@code historical_batch_log} 테이블에 상태 기록</li>
 *   <li>COMPLETED 날짜는 재실행 시 건너뜀</li>
 *   <li>FAILED 날짜는 다음 실행 시 재처리</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class HistoricalBatchService {

    private final HistoricalBatchProperties properties;
    private final HistoricalBatchRepository checkpointRepo;
    private final SyntheticConsultationFactory consultationFactory;

    private final AtomicBoolean running = new AtomicBoolean(false);

    public String runBatch() {
        if (!running.compareAndSet(false, true)) {
            return "이미 실행 중입니다.";
        }
        try {
            return executeBatch();
        } finally {
            running.set(false);
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    // ─── 메인 루프 ─────────────────────────────────────────────────────────────

    private String executeBatch() {
        LocalDate startDate = properties.getStartDate();
        LocalDate endDate   = properties.getEndDate();
        int dailyCount      = properties.getDailyCount();

        log.info("[HistoricalBatch] 시작 — {} ~ {}, 일일 {}건", startDate, endDate, dailyCount);

        // 날짜별 체크포인트 초기화 (이미 존재하는 날짜는 INSERT IGNORE)
        for (LocalDate d = startDate; !d.isAfter(endDate); d = d.plusDays(1)) {
            checkpointRepo.initDate(d, dailyCount);
        }

        List<LocalDate> targets = checkpointRepo.findPendingOrFailedDates();
        log.info("[HistoricalBatch] 처리 대상: {}일", targets.size());

        int success = 0, failed = 0;

        for (LocalDate targetDate : targets) {
            checkpointRepo.markInProgress(targetDate);
            try {
                processDate(targetDate);
                checkpointRepo.markCompleted(targetDate);
                success++;
                log.info("[HistoricalBatch] {} 완료 ({}/{}일)", targetDate, success, targets.size());
            } catch (Exception e) {
                failed++;
                log.error("[HistoricalBatch] {} 실패: {}", targetDate, e.getMessage(), e);
                checkpointRepo.markFailed(targetDate, e.getMessage());
            }
        }

        String result = String.format("완료 — 성공: %d일, 실패: %d일 (총 %d일)", success, failed, targets.size());
        log.info("[HistoricalBatch] {}", result);
        return result;
    }

    // ─── 날짜 단위 처리 ────────────────────────────────────────────────────────

    /**
     * 하루치 데이터를 chunkSize 단위로 나눠 생성한다.
     *
     * <p>각 청크는 독립 트랜잭션으로 커밋되며, 커밋 후 이벤트 상태를 REQUESTED로 등록한다.
     * AI 호출과 MongoDB 저장은 이 메서드에서 하지 않는다.
     */
    private void processDate(LocalDate targetDate) {
        int totalCount = properties.getDailyCount();
        int chunkSize  = properties.getChunkSize();
        int done       = 0;

        while (done < totalCount) {
            int size = Math.min(chunkSize, totalCount - done);

            // Step 1: consultation_results + raw_texts + 연관 테이블 생성
            //         @Transactional — 커밋 완료 후 반환
            SyntheticConsultationFactory.BatchResult result =
                    consultationFactory.executeStep1WithDate(size, targetDate);

            if (result.isEmpty()) {
                log.warn("[HistoricalBatch] {} — 데이터 생성 불가 (상담사/고객 없음)", targetDate);
                break;
            }

            // Step 2: result_event_status = REQUESTED
            //         → ExtractionScheduler가 감지해 Gemini API 호출
            consultationFactory.triggerAiExtraction(
                    result.consultIds(), result.categoryCodes());

            // Step 3: summary_event_status = requested
            //         → SummarySyncItemWriter가 감지해 MongoDB + ES 저장
            consultationFactory.triggerSummaryGeneration(result.consultIds());

            done += result.consultIds().size();
            checkpointRepo.updateProgress(targetDate, done, 0, 0);

            log.debug("[HistoricalBatch] {} {}/{}건 등록", targetDate, done, totalCount);
        }

        log.info("[HistoricalBatch] {} — {}건 REQUESTED 등록 완료 (AI·요약은 별도 배치 처리)",
                targetDate, done);
    }
}
