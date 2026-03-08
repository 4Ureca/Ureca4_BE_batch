package com.uplus.batch.jobs.monthly_agent_report.entity;

import com.uplus.batch.jobs.daily_agent_report.entity.CategoryRanking;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "monthly_agent_report_snapshot") // 월별 전용 컬렉션
public class MonthlyAgentReportSnapshot {

  @Id
  private String id;

  @Indexed
  private Long agentId;      // 상담사 식별자

  private LocalDate startAt;   // 집계 시작 시각 (해당 월 1일 00:00)
  private LocalDate endAt;     // 집계 종료 시각 (해당 월 말일 23:59)

  private long consultCount;   // 개인 상담 처리 건수
  private double avgConsultPerAgent; // 상담사 평균 처리 건수
  private double avgDurationMinutes; // 개인 평균 상담 소요 시간(분)
  private double customerSatisfaction; // 고객 만족도
  private double iamWriteRate;       // 상담 완료 대비 IAM 작성 비율(%)

  private List<CategoryRanking> categoryRanking; // 처리 카테고리 순위 리스트

  private QualityAnalysis qualityAnalysis;                // 상담사 응대 품질 분석
  private CustomerSatisfactionAnalysis customerSatisfactionAnalysis; // 고객 만족도 분석

  @CreatedDate
  private LocalDateTime createdAt; // 문서 생성 일시

  // --- 내부 계층 구조---

  @Data
  public static class QualityAnalysis {
    private long greetingCount;           // 인사말 포함 상담 건수
    private double greetingRate;          // 인사말 포함 비율(%)
    private long empathyExpressionCount;  // 공감 표현 총 횟수
    private double avgEmpathyPerConsult;  // 상담 1건당 평균 공감 표현 횟수
    private double personalizationRate;   // 고객 이름 포함 응대 비율(%)
    private double closingRate;           // 마무리 멘트 포함 비율(%)
    private double waitingGuideRate;      // 대기 안내 포함 비율(%)
    private double totalScore;            // 응대 품질 종합 점수 (0~100)
  }

  @Data
  public static class CustomerSatisfactionAnalysis {
    private List<String> satisfactionDetails; // 질문 항목에 대한 답변들 평균 리스트
  }
}