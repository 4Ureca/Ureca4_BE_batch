package com.uplus.batch.domain.summary.dto;

import java.time.LocalDateTime;

public record ConsultationResultSyncRow(
    Long consultId,
    LocalDateTime createdAt,
    String channel,
    Integer durationSec,
    String iamIssue,
    String iamAction,
    String iamMemo,

    Long employeeId,
    String employeeName,

    Long customerId,
    String customerName,
    String customerPhone,
    String ageGroup,
    String customerGrade,
    String customerType,
    String customerGender,

    String categoryCode,
    String categoryLarge,
    String categoryMedium,
    String categorySmall,

    String consultationType   // "IN" / "OUT"
) {
}
