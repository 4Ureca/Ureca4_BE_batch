package com.uplus.batch.domain.summary.dto;

import java.util.List;

public record RetentionAnalysisRow(
    Long consultId,
    Boolean hasIntent,
    Boolean defenseAttempted,
    Boolean defenseSuccess,
    List<String> defenseActions,
    String complaintReason,
    String rawSummary,

    String outboundCallResult,    // "CONVERTED" / "REJECTED" / null
    String outboundCategory,      // 거절 사유 코드 (COST, NO_NEED 등)
    String outboundReport         // AI 분석 텍스트
) {
}