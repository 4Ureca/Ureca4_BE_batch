package com.uplus.batch.jobs.summary_dummy.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ConsultationResultRow {

  private final Long consultId;
  private final LocalDateTime createdAt;

  private final Long empId;
  private final String agentName;

  private final Long customerId;
  private final String customerName;
  private final String customerType;
  private final String customerPhone;
  private final String gradeCode;
  private final LocalDate birthDate;

  private final String categoryCode;
  private final String categoryLarge;
  private final String categoryMedium;
  private final String categorySmall;

  private final String channel;
  private final Integer durationSec;

  private final String iamIssue;
  private final String iamAction;
  private final String iamMemo;
}