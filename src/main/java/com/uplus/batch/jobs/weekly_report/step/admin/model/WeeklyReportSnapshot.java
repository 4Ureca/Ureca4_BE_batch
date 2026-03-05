package com.uplus.batch.jobs.weekly_report.step.admin.model;

import java.time.LocalDateTime;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document(collection = "weekly_report_snapshot")
public class WeeklyReportSnapshot {
  @Id
  private String id;               // Mongo _id
  private LocalDateTime startAt;   // 집계 시작일
  private LocalDateTime endAt;     // 집계 종료일

  private SubscriptionAnalysis subscriptionAnalysis;

  @Data
  public static class SubscriptionAnalysis {
    private List<ProductCount> newSubscriptions;
    private List<ProductCount> canceledSubscriptions;
    private Double productSwitchRate;
    private List<SwitchDetail> productSwitchDetails;
    private List<ByAgeGroup> byAgeGroup;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class ByAgeGroup {
    private String ageGroup; // 연령대
    private List<ProductCount> preferredProducts; // 해당 연령대의 TOP 3 상품 리스트
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor // 몽고DB에서 데이터를 읽어올 때
  public static class ProductCount {
    private String productId;
    private String productName;
    private Long count;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class SwitchDetail {
    private String fromProductName;
    private String toProductName;
    private Long count;
  }
}
