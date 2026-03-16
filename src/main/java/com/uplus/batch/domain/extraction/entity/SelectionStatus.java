package com.uplus.batch.domain.extraction.entity;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum SelectionStatus {
    PENDING("검토 대기"),
    SELECTED("우수 사례 선정됨"),
    REJECTED("제외됨");

    private final String description;
}