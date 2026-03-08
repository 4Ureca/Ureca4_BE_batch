package com.uplus.batch.domain.extraction.entity;

public enum EventStatus {
    REQUESTED,   // 요약 요청됨
    PROCESSING,  // 처리 중
    COMPLETED,   // 완료
    FAILED       // 실패
}