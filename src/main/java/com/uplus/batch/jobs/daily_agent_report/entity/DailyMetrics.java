package com.uplus.batch.jobs.daily_agent_report.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class DailyMetrics {
  private long count;          // 건수
  private double avgDuration;  // 소요 시간(초)
  private Double avgSatisfaction; // 만족도
}