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

    @Column(nullable = false)
    private Boolean hasIntent;

    @Column(columnDefinition = "TEXT")
    private String complaintReason;

    @Column(nullable = false)
    private Boolean defenseAttempted;

    @Column(nullable = false)
    private Boolean defenseSuccess;

    @Column(columnDefinition = "json")
    private String defenseActions;

    @Column(columnDefinition = "TEXT") 
    private String rawSummary;

    @CreationTimestamp
    @Column(updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;

    private LocalDateTime deletedAt;

    @Builder
    public ConsultationExtraction(Long consultId, AiExtractionResponse res, String actionsJson) {
        this.consultId = consultId;
        this.hasIntent = res.has_intent();
        this.complaintReason = res.complaint_reason();
        this.defenseAttempted = res.defense_attempted();
        this.defenseSuccess = res.defense_success();
        this.rawSummary = res.raw_summary(); 
        this.defenseActions = actionsJson;
    }
}