package com.uplus.batch.jobs.monthly_report.step.admin.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;
import java.time.LocalDateTime;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document(collection = "monthly_report_snapshot") // 월별 전용 컬렉션
public class MonthlyReportSnapshot {
  @Id
  private String id;
  private LocalDateTime startAt;
  private LocalDateTime endAt;
  @CreatedDate
  private LocalDateTime createdAt;
  private SubscriptionAnalysis subscriptionAnalysis;
  private Object keywordSummary;  // 키워드 분석 (topKeywords, longTermTopKeywords, byCustomerType)

  @Data
  public static class SubscriptionAnalysis {
    private List<ProductCount> newSubscriptions;
    private List<ProductCount> canceledSubscriptions;
    private List<ByAgeGroup> byAgeGroup;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class ByAgeGroup {
    private String ageGroup; // 연령대
    private List<ProductCount> preferredProducts;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor // 몽고DB에서 데이터를 읽어올 때
  public static class ProductCount {
    private String productId;
    private String productName;
    private Long count;
  }
}
