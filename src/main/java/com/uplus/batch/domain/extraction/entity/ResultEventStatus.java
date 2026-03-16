package com.uplus.batch.domain.extraction.entity;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;

@Entity
@Table(name = "result_event_status")
@Getter 
@NoArgsConstructor(access = AccessLevel.PROTECTED) 
public class ResultEventStatus {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "result_event_id") 
    private Long resultEventId;

    @Column(name = "consult_id", nullable = false)
    private Long consultId;

    @Column(name = "category_code", length = 20, nullable = false)
    private String categoryCode;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 20, nullable = false)
    private EventStatus status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount = 0;

    @Column(name = "fail_reason", columnDefinition = "TEXT")
    private String failReason;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    @Column(name = "started_at")
    private LocalDateTime startedAt;

    @UpdateTimestamp // 수정 시 자동으로 시간 업데이트
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @Builder
    public ResultEventStatus(Long consultId, String categoryCode) {
        this.consultId = consultId;
        this.categoryCode = categoryCode;
        this.status = EventStatus.REQUESTED;
        this.retryCount = 0;
    }

    // --- 비즈니스 로직 (상태 제어) ---
    public void start() {
        this.status = EventStatus.PROCESSING;
        this.startedAt = LocalDateTime.now();
    }

    public void complete() {
        this.status = EventStatus.COMPLETED;
    }

    public void fail(String reason) {
        this.status = EventStatus.FAILED;
        this.failReason = reason;
        this.retryCount++;
    }
    public void retry() {
        this.status = EventStatus.REQUESTED;
        this.failReason = null; 
    }
}