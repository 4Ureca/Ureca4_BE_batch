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
  private Double avgSatisfaction; // 고객 만족도
  private long completedSurveyCount; // Aggregation에서 받아올 값
  private double responseRate; // 응답률

  private double avgIamMatchRate; // [추가] 쿼리 별명과 맞춤


  public void calculateResponseRate() {
    if (this.count > 0) {
      // (응답건수 / 전체건수) * 100
      this.responseRate = (double) this.completedSurveyCount / this.count * 100.0;
    } else {
      this.responseRate = 0.0;
    }
  }
}