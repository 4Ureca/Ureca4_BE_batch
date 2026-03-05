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

  private String agentId;      // 상담사 식별자 (ID)

  private LocalDate startAt;   // 집계 시작 일자 (해당일 00:00:00)

  private LocalDate endAt;     // 집계 종료 일자 (해당일 23:59:59)

  private long consultCount;   // 개인 상담 처리 건수 (전체)

  private double avgConsultPerAgent; // 상담사 평균 처리 건수 (전체 평균 대비 비교용)

  private double avgDurationMinutes; // 개인 평균 상담 소요 시간 (분 단위)

  private double iamWriteRate; // 상담 완료 대비 IAM 작성 비율 (%)

  /**
   * 처리 카테고리 순위 및 건수 리스트
   */
  private List<CategoryRanking> categoryRanking;

  /**
   * 상담 응대 품질 분석 결과 (인사말, 공감 등)
   */
  private QualityAnalysis qualityAnalysis;

  /**외부로 뺌**/
  // --- 내부 객체 정의 ---
//
//  @Getter
//  @Setter
//  @NoArgsConstructor
//  @AllArgsConstructor
//  @Builder
//  public static class CategoryRanking {
//    private String code;   // 카테고리 코드 (M_FEE_01 등)
//    private String large;  // 카테고리 대분류 (요금/납부 등)
//    private String mid;    // 카테고리 중분류 (요금제 변경 등)
//    private String small;  // 카테고리 소분류 (5G 요금제 안내 등)
//    private int count;     // 해당 카테고리 처리 건수 (중분류 기준 건수)
//    private int rank;      // 건수 기준 순위
//  }

  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  public static class QualityAnalysis {
    private int greetingCount;       // 인사말 포함 건수
    private double greetingRate;     // 인사말 포함 비율 (%)
    private int empathyExpressionCount; // 공감 표현 총 횟수
    // 필요한 추가 응대 품질 지표들...
  }
}