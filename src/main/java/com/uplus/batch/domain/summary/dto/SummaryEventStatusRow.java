package com.uplus.batch.domain.summary.dto;

public record SummaryEventStatusRow(
    Long id,
    Long consultId,
    Integer retryCount
) {
}
