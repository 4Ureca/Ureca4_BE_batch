package com.uplus.batch.domain.summary.entity;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

/**
 * summary_event_status 테이블 엔티티.
 * SyntheticDataGeneratorScheduler가 삽입 → ConsultationSummaryGenerator가 감지하여 처리.
 *
 * DDL (자동 생성 안 될 경우 수동 실행):
 * CREATE TABLE summary_event_status (
 *   id          BIGINT AUTO_INCREMENT PRIMARY KEY,
 *   consult_id  BIGINT       NOT NULL,
 *   status      ENUM('requested','completed','failed') NOT NULL,
 *   retry_count INT          NOT NULL DEFAULT 0,
 *   fail_reason TEXT,
 *   created_at  DATETIME     NOT NULL,
 *   updated_at  DATETIME
 * );
 */
@Entity
@Table(name = "summary_event_status")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class SummaryEventStatus {

    // DB 기존 DDL: PK 컬럼명 = "id" (summary_event_id가 아님)
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long summaryEventId;

    // DB DDL: unique 제약 없음 (복합 인덱스만 존재)
    @Column(name = "consult_id", nullable = false)
    private Long consultId;

    @Column(name = "status", nullable = false, length = 20)
    private String status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount = 0;

    @Column(name = "fail_reason", columnDefinition = "TEXT")
    private String failReason;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @Builder
    public SummaryEventStatus(Long consultId) {
        this.consultId = consultId;
        this.status = "requested";
        this.retryCount = 0;
    }

    public void start() {
        this.status = "requested";
    }

    public void complete() {
        this.status = "completed";
    }

    public void fail(String reason) {
        this.status = "failed";
        this.retryCount++;
        String trimmed = (reason != null && reason.length() > 200)
                ? reason.substring(0, 200) + "..." : reason;
        this.failReason = trimmed;
    }

    public void retry() {
        this.status = "requested";
        this.failReason = null;
    }

    /** JDBC RowMapper 전용 재구성 팩토리 — DB 조회 결과를 엔티티로 복원할 때 사용 */
    public static SummaryEventStatus reconstruct(Long id, Long consultId, String status,
                                                 int retryCount, String failReason,
                                                 LocalDateTime createdAt, LocalDateTime updatedAt) {
        SummaryEventStatus e = new SummaryEventStatus();
        e.summaryEventId = id;
        e.consultId      = consultId;
        e.status         = status;
        e.retryCount     = retryCount;
        e.failReason     = failReason;
        e.createdAt      = createdAt;
        e.updatedAt      = updatedAt;
        return e;
    }
}
