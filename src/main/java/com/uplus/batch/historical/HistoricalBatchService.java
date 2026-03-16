package com.uplus.batch.historical;

import com.uplus.batch.synthetic.OutboundConsultationFactory;
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
 *   인바운드: consultation_results + raw_texts + 연관 테이블 생성 (created_at = targetDate)
 *             result_event_status  → REQUESTED
 *             summary_event_status → requested
 *   아웃바운드: consultation_results + raw_texts + 연관 테이블 생성 (created_at = targetDate)
 *              result_event_status  → REQUESTED (category_code M_OTB_* → 아웃바운드 판별)
 *              summary_event_status → requested
 *
 * [ExtractionScheduler — 별도 배치]
 *   result_event_status=REQUESTED 감지 (인바운드 분)
 *   → Gemini API 호출 → retention_analysis 저장
 *   → result_event_status=COMPLETED
 *
 * [SummarySyncItemWriter — 별도 배치]
 *   summary_event_status=requested + result_event_status=COMPLETED 감지 (인바운드 분)
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
    private final OutboundConsultationFactory outboundConsultationFactory;

    private final AtomicBoolean running = new AtomicBoolean(false);

    public String runBatch() {
        return runBatch(properties.getDailyCount(), properties.getOutboundRatio());
    }

    public String runBatch(int dailyCount) {
        return runBatch(dailyCount, properties.getOutboundRatio());
    }

    public String runBatch(int dailyCount, int outboundRatio) {
        return runBatch(dailyCount, outboundRatio, false);
    }

    public String runBatch(int dailyCount, int outboundRatio, boolean resetCompleted) {
        if (!running.compareAndSet(false, true)) {
            return "이미 실행 중입니다.";
        }
        try {
            return executeBatch(dailyCount, outboundRatio, resetCompleted);
        } finally {
            running.set(false);
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    // ─── 메인 루프 ─────────────────────────────────────────────────────────────

    private String executeBatch(int dailyCount, int outboundRatio, boolean resetCompleted) {
        LocalDate startDate = properties.getStartDate();
        LocalDate endDate   = properties.getEndDate();

        int outboundPerDay = Math.round((float) dailyCount * outboundRatio / 100);
        int inboundPerDay  = dailyCount - outboundPerDay;

        log.info("[HistoricalBatch] 시작 — {} ~ {}, 일일 {}건 (인바운드 {}, 아웃바운드 {})",
                startDate, endDate, dailyCount, inboundPerDay, outboundPerDay);

        // reset=true면 COMPLETED 날짜를 PENDING으로 되돌린 후 재실행
        if (resetCompleted) {
            int reset = checkpointRepo.resetCompletedDates();
            log.info("[HistoricalBatch] COMPLETED → PENDING 초기화: {}일", reset);
        }

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
                processDate(targetDate, inboundPerDay, outboundPerDay);
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
     * 하루치 데이터를 인바운드·아웃바운드로 분리하여 chunkSize 단위로 생성한다.
     *
     * <ul>
     *   <li>인바운드: 각 청크를 독립 트랜잭션으로 커밋 후 AI·요약 이벤트 등록</li>
     *   <li>아웃바운드: 각 청크를 독립 트랜잭션으로 커밋 후 AI·요약 이벤트 등록</li>
     * </ul>
     */
    private void processDate(LocalDate targetDate, int inboundTotal, int outboundTotal) {
        int chunkSize = properties.getChunkSize();

        int inboundDone = processInbound(targetDate, inboundTotal, chunkSize);
        int outboundDone = processOutbound(targetDate, outboundTotal, chunkSize);

        checkpointRepo.updateProgress(targetDate, inboundDone + outboundDone, 0, 0);

        log.info("[HistoricalBatch] {} — 인바운드 {}건 + 아웃바운드 {}건 등록 완료",
                targetDate, inboundDone, outboundDone);
    }

    private int processInbound(LocalDate targetDate, int total, int chunkSize) {
        int done = 0;
        while (done < total) {
            int size = Math.min(chunkSize, total - done);

            SyntheticConsultationFactory.BatchResult result =
                    consultationFactory.executeStep1WithDate(size, targetDate);

            if (result.isEmpty()) {
                log.warn("[HistoricalBatch] {} — 인바운드 데이터 생성 불가 (상담사/고객 없음)", targetDate);
                break;
            }

            consultationFactory.triggerAiExtraction(result.consultIds(), result.categoryCodes());
            consultationFactory.triggerSummaryGeneration(result.consultIds());

            done += result.consultIds().size();
            log.debug("[HistoricalBatch] {} 인바운드 {}/{}건 등록", targetDate, done, total);
        }
        return done;
    }

    private int processOutbound(LocalDate targetDate, int total, int chunkSize) {
        int done = 0;
        while (done < total) {
            int size = Math.min(chunkSize, total - done);

            OutboundConsultationFactory.BatchResult result =
                    outboundConsultationFactory.executeStep1WithDate(size, targetDate);

            if (result.isEmpty()) {
                log.warn("[HistoricalBatch] {} — 아웃바운드 데이터 생성 불가 (상담사/고객 없음)", targetDate);
                break;
            }

            outboundConsultationFactory.triggerAiExtraction(result.consultIds(), result.categoryCodes());
            consultationFactory.triggerExcellentScoring(result.consultIds());
            outboundConsultationFactory.triggerSummaryGeneration(result.consultIds());
            done += result.consultIds().size();
            log.debug("[HistoricalBatch] {} 아웃바운드 {}/{}건 등록", targetDate, done, total);
        }
        return done;
    }
}
