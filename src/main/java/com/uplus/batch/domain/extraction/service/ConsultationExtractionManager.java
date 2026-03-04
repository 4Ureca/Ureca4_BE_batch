package com.uplus.batch.domain.extraction.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.uplus.batch.domain.extraction.dto.AiExtractionResponse;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class ConsultationExtractionManager {
    private final GeminiExtractor geminiExtractor;
    // PatternExtractor는 우선 AI 추출 결과와 동일한 규격의 빈 결과를 반환하게 만듭니다.

    public AiExtractionResponse runExtraction(String categoryCode, String rawIssue) {
        // '해지' 키워드가 들어간 카테고리만 AI 활용
        if (categoryCode != null && categoryCode.contains("CHN")) {
            return geminiExtractor.extract(rawIssue);
        }
        
        // 그 외 일반 카테고리는 기본값 반환 (로직 추가 필요)
        return new AiExtractionResponse(false, "일반 상담", false, false, List.of(), "단순 문의");
    }
}