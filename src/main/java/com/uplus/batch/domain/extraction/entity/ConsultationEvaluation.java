package com.uplus.batch.domain.extraction.entity;

import java.time.LocalDateTime;
import org.hibernate.annotations.CreationTimestamp;
import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "consultation_evaluations")
@Getter @NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ConsultationEvaluation {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long evaluationId;

    @Column(nullable = false)
    private Long consultId;

    private int score;

    @Column(columnDefinition = "TEXT")
    private String evaluationReason;

    private boolean isCandidate;
    @Enumerated(EnumType.STRING)
    @Column(length = 20)
    private SelectionStatus selectionStatus;

    @CreationTimestamp
    private LocalDateTime createdAt;

    @Builder
    public ConsultationEvaluation(Long consultId, int score, String evaluationReason, boolean isCandidate) {
        this.consultId = consultId;
        this.score = score;
        this.evaluationReason = evaluationReason;
        this.isCandidate = isCandidate;
        // ✅ 후보군(isCandidate=true)인 경우에만 PENDING으로 시작, 아니면 REJECTED나 NULL 처리
        this.selectionStatus = isCandidate ? SelectionStatus.PENDING : SelectionStatus.REJECTED;
    }
}