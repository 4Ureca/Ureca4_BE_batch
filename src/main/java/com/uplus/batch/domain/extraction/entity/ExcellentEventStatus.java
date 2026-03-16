package com.uplus.batch.domain.extraction.entity;

import java.time.LocalDateTime;

import org.hibernate.annotations.UpdateTimestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "excellent_event_status")
@Getter @NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ExcellentEventStatus {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long excellentEventId;

    @Column(nullable = false)
    private Long consultId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private EventStatus status; // REQUESTED, PROCESSING, COMPLETED, FAILED

    private int retryCount = 0;
    private String failReason;
    
    @Column(updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();
    private LocalDateTime startedAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;

    @Builder
    public ExcellentEventStatus(Long consultId) {
        this.consultId = consultId;
        this.status = EventStatus.REQUESTED;
    }

    public void start() {
        this.status = EventStatus.PROCESSING;
        this.startedAt = LocalDateTime.now();
    }

    public void complete() { this.status = EventStatus.COMPLETED; }

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