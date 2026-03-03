package com.uplus.batch.domain.summary.entity;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.IndexDirection;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;
import java.util.List;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "consultation_summary")
@CompoundIndex(name = "idx_consultedAt_agentId",
    def = "{'consultedAt':-1,'agent.id':1}")
public class ConsultationSummary {

  @Id
  private String id; // _id (ObjectId)

  @Indexed(name = "uq_consultId", unique = true)
  private Long consultId;

  @Indexed(name = "idx_consultedAt", direction = IndexDirection.DESCENDING)
  private LocalDateTime consultedAt;

  private String channel;
  private Integer durationSec;
  private Agent agent;
  private Category category;
  private Iam iam;
  private Summary summary;
  private List<String> riskFlags;
  private Customer customer;
  private Cancellation cancellation;
  private ResultProducts resultProducts;
  private LocalDateTime createdAt;

  // ================= Embedded =================

  @Getter @Setter @Builder @NoArgsConstructor @AllArgsConstructor
  public static class Agent {
    private Long id;
    private String name;
  }

  @Getter @Setter @Builder @NoArgsConstructor @AllArgsConstructor
  public static class Category {
    private String code;
    private String large;
    private String medium;
    private String small;
  }

  @Getter @Setter @Builder @NoArgsConstructor @AllArgsConstructor
  public static class Iam {
    private String issue;
    private String action;
    private String memo;
    private List<String> matchKeyword;
    private Double matchRates;
  }

  @Getter @Setter @Builder @NoArgsConstructor @AllArgsConstructor
  public static class Summary {
    private String status;        // PENDING / COMPLETED / FAILED
    private String content;       // AI 한줄요약
    private List<String> keywords;
  }

  @Getter @Setter @Builder @NoArgsConstructor @AllArgsConstructor
  public static class Customer {
    private Long id;
    private String type;
    private String phone;
    private String name;
    private String ageGroup;
    private String grade;
    private Double satisfiledScore;
  }

  @Getter @Setter @Builder @NoArgsConstructor @AllArgsConstructor
  public static class Cancellation {
    private Boolean intent;
    private Boolean defenseAttempted;
    private Boolean defenseSuccess;
    private List<String> defenseActions;
    private String complaintReasons;
  }

  @Getter @Setter @Builder @NoArgsConstructor @AllArgsConstructor
  public static class ResultProducts {
    private List<String> subscribed;
    private List<String> canceled;
    private List<Conversion> conversion;
    private List<String> recommitment;
    private String changeType;

    @Getter @Setter @Builder @NoArgsConstructor @AllArgsConstructor
    public static class Conversion {
      private String subscribed;
      private String canceled;
    }
  }
}