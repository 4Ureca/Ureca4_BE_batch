package com.uplus.batch.domain.extraction.schedular;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.extraction.dto.AiExtractionResponse;
import com.uplus.batch.domain.extraction.dto.BundledAiResult;
import com.uplus.batch.domain.extraction.entity.ConsultationExtraction;
import com.uplus.batch.domain.extraction.entity.ConsultationRawText;
import com.uplus.batch.domain.extraction.entity.EventStatus;
import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.repository.ConsultationExtractionRepository;
import com.uplus.batch.domain.extraction.repository.ConsultationRawTextRepository;
import com.uplus.batch.domain.extraction.repository.EventStatusRepository;
import com.uplus.batch.domain.extraction.service.BundledGeminiExtractor;
import com.uplus.batch.domain.extraction.service.BundledGeminiExtractor.BundleItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * AI 한 줄 요약 추출 스케줄러.
 *
 * <h3>번들 처리 방식 (비용 최적화)</h3>
 * <pre>
 * 1. result_event_status=REQUESTED 최대 50건 조회
 * 2. 20건씩 분할 → 1회 Gemini API 호출로 20건 동시 처리
 *    (기존 1건/호출 대비 최대 20배 비용 절감)
 * 3. 번들 내 각 건 결과를 개별 트랜잭션으로 저장 (실패 격리)
 * </pre>
 *
 * <h3>장애 처리</h3>
 * <ul>
 *   <li>번들 API 호출 자체 실패 → 번들 전체 FAILED (RetryScheduler 재처리)</li>
 *   <li>번들 내 개별 저장 실패 → 해당 건만 FAILED (나머지 성공 건 유지)</li>
 *   <li>API 재시도: 지수 백오프 3회 (BundledGeminiExtractor 내장)</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ExtractionScheduler {

    private static final int BUNDLE_SIZE = 20;

    private final EventStatusRepository            eventRepository;
    private final ConsultationRawTextRepository    rawTextRepository;
    private final ConsultationExtractionRepository extractionRepository;
    private final BundledGeminiExtractor           bundledExtractor;
    private final ObjectMapper                     objectMapper;

    @Scheduled(fixedDelayString = "${extraction.fixed-delay:60000}")
    public void executeExtractionJob() {
        List<ResultEventStatus> pendingTasks =
                eventRepository.findTop50ByStatusOrderByCreatedAtAsc(EventStatus.REQUESTED);

        if (pendingTasks.isEmpty()) {
            log.info("[Batch] 처리할 REQUESTED 데이터 없음");
            return;
        }

        log.info("[Batch] 추출 시작 — {}건 ({}건씩 번들)", pendingTasks.size(), BUNDLE_SIZE);

        Map<String, List<ResultEventStatus>> byType = pendingTasks.stream()
                .collect(Collectors.groupingBy(t ->
                        t.getCategoryCode() != null && t.getCategoryCode().startsWith("M_OTB")
                                ? "OUTBOUND" : "INBOUND"
                ));

        for (List<ResultEventStatus> typeGroup : byType.values()) {
            for (List<ResultEventStatus> bundle : partition(typeGroup, BUNDLE_SIZE)) {
                try {
                    processBundledTasks(bundle);
                } catch (Exception e) {
                    log.error("[Batch] 번들 처리 중 예기치 못한 오류: {}", e.getMessage());
                }
            }
        }

        log.info("[Batch] 추출 완료");
    }

    // ─── 번들 단위 처리 ───────────────────────────────────────────────────────

    private void processBundledTasks(List<ResultEventStatus> bundle) {

        // 1. 선점 — 모두 PROCESSING으로 변경 (중복 처리 방지)
        for (ResultEventStatus task : bundle) {
            markAsProcessing(task);
        }

        // 2. raw_text_json 일괄 조회 (IN 쿼리 1번)
        List<Long> consultIds = bundle.stream()
                .map(ResultEventStatus::getConsultId).toList();

        Map<Long, String> rawTextMap = rawTextRepository
                .findAllByConsultIdIn(consultIds)
                .stream()
                .collect(Collectors.toMap(
                        ConsultationRawText::getConsultId,
                        ConsultationRawText::getRawTextJson,
                        (a, b) -> a
                ));

        // 3. 번들 아이템 구성
        List<BundleItem> items = bundle.stream()
                .map(task -> new BundleItem(
                        task.getConsultId(),
                        task.getCategoryCode(),
                        rawTextMap.getOrDefault(task.getConsultId(), "")
                ))
                .toList();

        // 4. 번들 AI 호출 — 20건 → Gemini 1회 (지수 백오프 내장)
        List<BundledAiResult> aiResults;
        try {
            aiResults = bundledExtractor.extractBatch(items, 3, 5000);
        } catch (Exception e) {
            // API 자체 실패 → 번들 전체 FAILED (RetryScheduler가 재배치)
            log.error("[Batch] 번들 AI 호출 실패 → {}건 전체 FAILED: {}", bundle.size(), e.getMessage());
            for (ResultEventStatus task : bundle) {
                markAsFailed(task, e.getMessage());
            }
            return;
        }

        // 5. 결과 저장 — 건별 독립 처리 (한 건 실패가 나머지에 영향 없음)
        Map<Long, BundledAiResult> resultMap = aiResults.stream()
                .collect(Collectors.toMap(BundledAiResult::consultId, r -> r, (a, b) -> a));

        for (ResultEventStatus task : bundle) {
            saveExtractionResult(task, resultMap.get(task.getConsultId()));
        }
    }

    // ─── 개별 결과 저장 (@Transactional REQUIRES_NEW) ─────────────────────────

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveExtractionResult(ResultEventStatus task, BundledAiResult aiResult) {
        try {
            if (aiResult == null || !aiResult.isSuccess()) {
                task.fail("AI 응답 없음 또는 raw_summary 비어있음");
                eventRepository.saveAndFlush(task);
                return;
            }

            if (aiResult.rawSummary() == null || aiResult.rawSummary().isBlank()) {
                task.fail("AI가 내용을 생성하지 못했습니다.");
                eventRepository.saveAndFlush(task);
                return;
            }

            boolean isOutbound = task.getCategoryCode() != null && task.getCategoryCode().startsWith("M_OTB");

            AiExtractionResponse extractionRes;
            String actionsJson;
            String complaintCategory;
            String defenseCategory;
            String outboundCallResult;
            String outboundReport;
            String outboundCategory;

            if (isOutbound) {
                // 아웃바운드: has_intent/defense_attempted/defense_success → NULL (인바운드 전용 컬럼)
                extractionRes = new AiExtractionResponse(
                        null,       // has_intent — 아웃바운드 미사용
                        null,       // complaint_reason — 아웃바운드 미사용
                        null,       // defense_attempted — 아웃바운드 미사용
                        null,       // defense_success — 아웃바운드 미사용
                        List.of(),  // defense_actions — 아웃바운드 미사용
                        aiResult.rawSummary()
                );
                actionsJson       = "[]";
                complaintCategory = null;
                defenseCategory   = null;
                outboundCallResult = aiResult.outboundCallResult();
                outboundReport     = aiResult.outboundReport();
                outboundCategory   = aiResult.outboundCategory(); // fallback은 BundledGeminiExtractor에서 처리됨
            } else {
                // 인바운드: AI 응답 그대로 사용
                extractionRes = aiResult.toAiExtractionResponse();
                actionsJson   = objectMapper.writeValueAsString(
                        aiResult.defenseActions() == null ? List.of() : aiResult.defenseActions()
                );
                complaintCategory  = aiResult.complaintCategory();
                defenseCategory    = aiResult.defenseCategory();
                outboundCallResult = null;
                outboundReport     = null;
                outboundCategory   = null;
            }

            ConsultationExtraction extraction = ConsultationExtraction.builder()
                    .consultId(task.getConsultId())
                    .res(extractionRes)
                    .actionsJson(actionsJson)
                    .complaintCategory(complaintCategory)
                    .defenseCategory(defenseCategory)
                    .outboundCallResult(outboundCallResult)
                    .outboundReport(outboundReport)
                    .outboundCategory(outboundCategory)
                    .build();

            extractionRepository.save(extraction);

            task.complete();
            eventRepository.saveAndFlush(task);
            log.info("[Success] 상담 ID {} 추출 완료", task.getConsultId());

        } catch (Exception e) {
            log.error("[Task Failed] ID {}: {}", task.getConsultId(), e.getMessage());
            task.fail(trim(e.getMessage()));
            eventRepository.saveAndFlush(task);
        }
    }

    // ─── 상태 변경 헬퍼 ───────────────────────────────────────────────────────

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markAsProcessing(ResultEventStatus task) {
        task.start();
        eventRepository.saveAndFlush(task);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markAsFailed(ResultEventStatus task, String reason) {
        task.fail(trim(reason));
        eventRepository.saveAndFlush(task);
    }

    // ─── 유틸 ─────────────────────────────────────────────────────────────────

    /** 리스트를 size 크기로 분할한다 */
    private <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> result = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            result.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return result;
    }

    private String trim(String msg) {
        if (msg == null) return null;
        return msg.length() > 200 ? msg.substring(0, 200) + "..." : msg;
    }
}
