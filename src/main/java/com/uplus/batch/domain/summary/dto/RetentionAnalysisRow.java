package com.uplus.batch.domain.summary.dto;

import java.util.List;

public record RetentionAnalysisRow(
    Long consultId,
    Boolean hasIntent,
    Boolean defenseAttempted,
    Boolean defenseSuccess,
    List<String> defenseActions,
    String complaintReason,
    String rawSummary
) {
}