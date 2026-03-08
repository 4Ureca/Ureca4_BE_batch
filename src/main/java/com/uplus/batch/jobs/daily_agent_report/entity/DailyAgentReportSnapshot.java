package com.uplus.batch.jobs.daily_agent_report.entity;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDate;
import java.util.List;

/**
 * 상담사 일별 리포트 스냅샷
 * 상담사 ID와 날짜를 복합 인덱스로 설정하여 조회 성능을 높임
 */
@Document(collection = "daily_agent_report_snapshot")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
@CompoundIndex(name = "agent_startAt_idx", def = "{'agentId': 1, 'startAt': 1}", unique = true)
public class DailyAgentReportSnapshot {

  @Id
  private String id;

  private Long agentId;      // 상담사 식별자 (ID)

  private LocalDate startAt;   // 집계 시작 일자 (해당일 00:00:00)

  private LocalDate endAt;     // 집계 종료 일자 (해당일 23:59:59)

  private long consultCount;   // 개인 상담 처리 건수 (전체)

  private double avgConsultPerAgent; // 상담사 평균 처리 건수 (전체 평균 대비 비교용)

  private double avgDurationMinutes; // 개인 평균 상담 소요 시간 (분 단위)

  private double customerSatisfaction; // 고객 만족도

  private double iamWriteRate; // 상담 완료 대비 IAM 작성 비율 (%)

  /**
   * 처리 카테고리 순위 및 건수 리스트
   */
  private List<CategoryRanking> categoryRanking;

  /**
   * 상담 응대 품질 분석 결과 (인사말, 공감 등)
   */
  private QualityAnalysis qualityAnalysis;


  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  public static class QualityAnalysis { // 응대 품질 기능 구현할 때 보완필요
    private int greetingCount;       // 인사말 포함 건수
    private double greetingRate;     // 인사말 포함 비율 (%)
    private int empathyExpressionCount; // 공감 표현 총 횟수
  }
}