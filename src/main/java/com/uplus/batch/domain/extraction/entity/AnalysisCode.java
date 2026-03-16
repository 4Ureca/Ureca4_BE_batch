package com.uplus.batch.domain.extraction.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "analysis_code")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class AnalysisCode {
    @Id
    private Long id;
    
    @Column(name = "code_name")
    private String codeName;
    
    private String classification; // complaint_category, defense_category, outbound_category
    
    private String description;
}