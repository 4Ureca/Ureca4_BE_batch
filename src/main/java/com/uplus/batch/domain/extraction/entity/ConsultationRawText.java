package com.uplus.batch.domain.extraction.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Entity
@Table(name = "consultation_raw_texts")
@Getter @NoArgsConstructor
public class ConsultationRawText {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long rawId;

    @Column(nullable = false)
    private Long consultId;

    @Column(columnDefinition = "json", nullable = false)
    private String rawTextJson; // 화자별 대화 내용이 담긴 JSON

    private LocalDateTime createdDate;
    private LocalDateTime updatedAt;
    private LocalDateTime deletedAt;
}