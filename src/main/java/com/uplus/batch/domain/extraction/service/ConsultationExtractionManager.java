package com.uplus.batch.domain.extraction.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.uplus.batch.domain.extraction.dto.AiExtractionResponse;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class ConsultationExtractionManager {
    private final GeminiExtractor geminiExtractor;

    public AiExtractionResponse runExtraction(String categoryCode, String rawIssue) {
        boolean isTerminationMode = (categoryCode != null && categoryCode.contains("CHN"));
        return geminiExtractor.extract(rawIssue, isTerminationMode);
    }
}