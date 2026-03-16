package com.uplus.batch.domain.extraction.dto;

import java.util.List;
import java.util.Collections;

public record AiExtractionResponse(
    // 1. [공통] 모든 분석 모드에서 추출
    String raw_summary,

    // 2. [CHN 전용] 인바운드 해지방어 분석 시에만 추출
    Boolean has_intent,
    String complaint_reason,
    String complaint_category,
    Boolean defense_attempted,
    Boolean defense_success,
    List<String> defense_actions,
    String defense_category,

    // 3. [OTB 전용] 아웃바운드 이탈방어 분석 시에만 추출
    String outbound_call_result,
    String outbound_report,
    String outbound_category
) {
    public AiExtractionResponse {
        if (defense_actions == null) defense_actions = Collections.emptyList();
    }
}