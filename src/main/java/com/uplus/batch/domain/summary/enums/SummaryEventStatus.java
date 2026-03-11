package com.uplus.batch.domain.summary.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum SummaryEventStatus {

  REQUESTED("requested"),
  COMPLETED("completed"),
  FAILED("failed");

  private final String value;
}