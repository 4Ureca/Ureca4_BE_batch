package com.uplus.batch.domain.extraction.dto;

import java.util.List;

/**
 * 번들 Gemini API 호출 결과 — 상담 1건에 대한 파싱 결과.
 *
 * <p>ExtractionScheduler가 20건을 1번의 API 호출로 처리할 때 사용한다.
 */
public record BundledAiResult(
        int index,
        long consultId,
        String categoryCode,
        boolean hasIntent,
        String complaintReason,
        boolean defenseAttempted,
        boolean defenseSuccess,
        List<String> defenseActions,
        String rawSummary,
        String complaintCategory,
        String defenseCategory,
        // 아웃바운드 전용 (인바운드에서는 null)
        String outboundCallResult,
        String outboundReport,
        String outboundCategory
) {
    /** AiExtractionResponse로 변환 — ConsultationExtraction.builder()에 전달용 */
    public AiExtractionResponse toAiExtractionResponse() {
        return new AiExtractionResponse(
                hasIntent,
                complaintReason,
                defenseAttempted,
                defenseSuccess,
                defenseActions == null ? List.of() : defenseActions,
                rawSummary
        );
    }

    /** raw_summary가 존재하고 AI 분석 누락이 아닌 경우 성공으로 판단 */
    public boolean isSuccess() {
        return rawSummary != null
                && !rawSummary.isBlank()
                && !rawSummary.contains("AI 분석 누락");
    }
}
