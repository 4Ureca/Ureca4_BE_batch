package com.uplus.batch.jobs.weekly_agent_report.entity;

import com.uplus.batch.jobs.daily_agent_report.entity.CategoryRanking;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Getter
@Setter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Document(collection = "weekly_agent_report_snapshot") // 주간 전용 컬렉션 명시
@CompoundIndex(name = "agent_startAt_idx", def = "{'agentId': 1, 'startAt': 1}", unique = true) // 중복 방지
public class WeeklyAgentReportSnapshot {

  @Id
  private String id;

  @Indexed
  private Long agentId;      // 상담사 식별자
  private String agentName;  // 상담사 이름

  private LocalDateTime startAt;   // 집계 시작 시각
  private LocalDateTime endAt;     // 집계 종료 시각

  private long consultCount;   // 개인 상담 처리 건수

  private double avgDurationMinutes; // 평균 소요 시간

  private double iamKeywordMatchAnalysis; // 주간 키워드 일치율

  private List<CategoryRanking> categoryRanking; // 처리 카테고리 순위

  private QualityAnalysis qualityAnalysis;                // 상담사 응대 품질 분석
  private CustomerSatisfactionAnalysis customerSatisfactionAnalysis; // 고객 만족도 분석

  @CreatedDate
  private LocalDateTime createdAt; // 생성일시


  // --- 내부 계층 구조 클래스들 ---

  @Data
  public static class QualityAnalysis {
    private long empathyCount;            // 공감 표현 등장 총 횟수
    private double avgEmpathyPerConsult;  // 건당 평균 공감 횟수
    private double apologyRate;           // 사과 표현 포함 비율 (%)
    private double closingRate;           // 마무리 멘트 포함 비율 (%)
    private double courtesyRate;          // 친절 표현 포함 비율 (%)
    private double promptnessRate;        // 신속 응대 표현 포함 비율 (%)
    private double accuracyRate;          // 정확 응대 표현 포함 비율 (%)
    private double waitingGuideRate;      // 대기 안내 포함 비율 (%)
    private long analyzedCount;           // 분석 대상 상담 건수
    private double totalScore;            // 종합 점수 (0~5)
  }

  @Data
  @Builder
  @AllArgsConstructor
  public static class CustomerSatisfactionAnalysis {
    private double satisfactionScore; // 질문 항목에 대한 답변들 평균
    private double responseRate; // 응답률
    private long surveyTotalCount;    // 설문 요청 수
    private long surveyResponseCount; // 설문 응답 수
  }
}