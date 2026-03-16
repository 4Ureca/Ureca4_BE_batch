package com.uplus.batch.domain.extraction.entity;

import com.uplus.batch.domain.extraction.dto.AiExtractionResponse;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "retention_analysis")
@Getter 
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ConsultationExtraction {

    @Id
    @Column(name = "consult_id")
    private Long consultId;

    // --- [1. 인바운드/해지 관련 필드] ---
    // 분석 모드에 따라 null일 수 있으므로 primitive(boolean) 대신 wrapper(Boolean) 사용
    private Boolean hasIntent;

    @Column(columnDefinition = "TEXT")
    private String complaintReason;

    @Column(length = 20)
    private String complaintCategory; // varchar(20)

    private Boolean defenseAttempted;

    private Boolean defenseSuccess;

    @Column(length = 20)
    private String defenseCategory; // varchar(20)

    @Column(columnDefinition = "json")
    private String defenseActions;

    // --- [2. 아웃바운드 전용 필드] ---
    @Column(length = 20)
    private String outboundCallResult; // enum('CONVERTED','REJECTED') 대용 varchar(20)

    @Column(columnDefinition = "TEXT")
    private String outboundReport; // text

    @Column(length = 20)
    private String outboundCategory; // varchar(20)

    // --- [3. 공통 필드] ---
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
        
        // 공통
        this.rawSummary = res.raw_summary();

        // 인바운드/해지 (CHN 모드)
        this.hasIntent = res.has_intent();
        this.complaintReason = res.complaint_reason();
        this.complaintCategory = res.complaint_category();
        this.defenseAttempted = res.defense_attempted();
        this.defenseSuccess = res.defense_success();
        this.defenseCategory = res.defense_category();
        this.defenseActions = actionsJson;

        // 아웃바운드 (OTB 모드)
        this.outboundCallResult = res.outbound_call_result();
        this.outboundReport = res.outbound_report();
        this.outboundCategory = res.outbound_category();
    }
}