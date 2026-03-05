package com.uplus.batch.jobs.daily_agent_report.entity;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CategoryRanking {
  private String code;
  private String large;
  private String medium;
  private int count;
  private int rank;
}
