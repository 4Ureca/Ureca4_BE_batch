package com.uplus.batch.domain.extraction.entity;

import com.uplus.batch.domain.extraction.dto.AiExtractionResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "retention_analysis")
@Getter 
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ConsultationExtraction {

    @Id
    @Column(name = "consult_id") // PK이자 FK (기존 상담 식별자)
    private Long consultId;

    @Column
    private Boolean hasIntent;

    @Column(columnDefinition = "TEXT")
    private String complaintReason;

    @Column
    private Boolean defenseAttempted;

    @Column
    private Boolean defenseSuccess;

    @Column(columnDefinition = "json")
    private String defenseActions;

    @Column(columnDefinition = "TEXT")
    private String rawSummary;

    @Column(name = "complaint_category", length = 30)
    private String complaintCategory;

    @Column(name = "defense_category", length = 30)
    private String defenseCategory;

    /** 아웃바운드 전용 — CONVERTED | REJECTED */
    @Column(name = "outbound_call_result", length = 20)
    private String outboundCallResult;

    /** 아웃바운드 전용 — AI가 생성한 자연어 상담 결과 보고서 */
    @Column(name = "outbound_report", columnDefinition = "TEXT")
    private String outboundReport;

    /** 아웃바운드 전용 — analysis_code.outbound_category FK (REJECTED 시만 값 존재) */
    @Column(name = "outbound_category", length = 20)
    private String outboundCategory;

    @CreationTimestamp
    @Column(updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;

    private LocalDateTime deletedAt;

    @Builder
    public ConsultationExtraction(Long consultId, AiExtractionResponse res, String actionsJson,
                                  String complaintCategory, String defenseCategory,
                                  String outboundCallResult, String outboundReport, String outboundCategory) {
        this.consultId = consultId;
        this.hasIntent = res.has_intent();
        this.complaintReason = res.complaint_reason();
        this.defenseAttempted = res.defense_attempted();
        this.defenseSuccess = res.defense_success();
        this.rawSummary = res.raw_summary();
        this.defenseActions = actionsJson;
        this.complaintCategory = complaintCategory;
        this.defenseCategory = defenseCategory;
        this.outboundCallResult = outboundCallResult;
        this.outboundReport = outboundReport;
        this.outboundCategory = outboundCategory;
    }
}