package com.uplus.batch.domain.summary.dto;

public record CustomerReviewRow(
    Long consultId,
    Integer score1,
    Integer score2,
    Integer score3,
    Integer score4,
    Integer score5
) {
}