package com.uplus.batch.jobs.daily_agent_report.entity;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
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

  @Indexed
  private Long agentId;      // 상담사 식별자 (ID)

  private LocalDate startAt;   // 집계 시작 일자

  private LocalDate endAt;     // 집계 종료 일자

  private long consultCount;   // 개인 상담 처리 건수 (전체)

  private double avgDurationMinutes; // 개인 평균 상담 소요 시간 (분 단위)

  private double iamKeywordMatchAnalysis; // 키워드 일치율

   //처리 카테고리 순위 및 건수 리스트
  private List<CategoryRanking> categoryRanking;

  //상담 응대 품질 분석 결과 (인사말, 공감 등)
  private QualityAnalysis qualityAnalysis;


  private CustomerSatisfactionAnalysis customerSatisfactionAnalysis; // 고객 만족도



  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
<<<<<<< feat/CV4-80
  public static class QualityAnalysis {
    private int analyzedCount;            // 실제 분석 완료 상담 건수 (주별/월별 가중 평균 분모용)
    private long empathyCount;            // 공감 표현 등장 총 횟수
    private double avgEmpathyPerConsult;  // 건당 평균 공감 횟수
    private double apologyRate;           // 사과 표현 포함 비율 (%)
    private double closingRate;           // 마무리 멘트 포함 비율 (%)
    private double courtesyRate;          // 친절 표현 포함 비율 (%)
    private double promptnessRate;        // 신속 응대 표현 포함 비율 (%)
    private double accuracyRate;          // 정확 응대 표현 포함 비율 (%)
    private double waitingGuideRate;      // 대기 안내 포함 비율 (%)
    private double totalScore;            // 종합 점수 (0~5)
=======
  public static class QualityAnalysis { // 응대 품질 기능 구현할 때 보완필요
    private int greetingCount;       // 인사말 포함 건수
    private double greetingRate;     // 인사말 포함 비율 (%)
    private int empathyExpressionCount; // 공감 표현 총 횟수
    private int avgEmpathyPerConsult; //상담 1건당 평균 공감 표현 횟수
    private double closingRate; // 마무리 멘트 포함 비율
    private double waitingGuideRate; // 대기 안내 멘트 포함 비율
    private double totalScore; // 종합 접수 (0~5)

  }

  @Data
  @Builder
  @AllArgsConstructor
  @Getter
  public static class CustomerSatisfactionAnalysis {
    private double satisfactionScore; // 질문 항목에 대한 답변들 평균
    private double responseRate; // 응답률

    // [중요] 주간/월간 가중 평균을 위해 꼭 필요한 필드들
    private int surveyTotalCount;       // 오늘의 설문 요청 총 건수
    private int surveyResponseCount;    // 오늘의 설문 응답 건수
>>>>>>> develop
  }
}
