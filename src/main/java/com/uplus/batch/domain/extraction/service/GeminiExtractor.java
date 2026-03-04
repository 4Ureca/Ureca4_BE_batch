package com.uplus.batch.domain.extraction.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.extraction.dto.AiExtractionResponse;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class GeminiExtractor {

    @Value("${gemini.api.key}")
    private String apiKey;

    private final ObjectMapper objectMapper; 
    private final RestClient restClient = RestClient.builder()
            .requestFactory(new org.springframework.http.client.SimpleClientHttpRequestFactory() {{
                setConnectTimeout(5000);
                setReadTimeout(30000);
            }})
            .build();

    private static final String MODEL_NAME = "gemini-2.5-flash"; 
    
    //재시도 설정 (네트워크 에러 시 2초 간격으로 최대 3번 시도)
    @Retryable( 							
        retryFor = {Exception.class}, 
        maxAttempts = 3, 
        backoff = @Backoff(delay = 2000)
    )
    public AiExtractionResponse extract(String rawIssue) {
        if (rawIssue == null || rawIssue.isBlank()) {
            throw new IllegalArgumentException("분석할 상담 원문이 비어 있습니다.");
        }

        String url = "https://generativelanguage.googleapis.com/v1beta/models/" + MODEL_NAME + ":generateContent?key=" + apiKey;

        String prompt = String.format("""
            당신은 상담 분석 전문가입니다. 다음 상담 원문을 분석하여 JSON 데이터만 출력하세요.
            반드시 아래의 JSON 형식을 엄수하며, 다른 설명이나 마크다운 기호는 생략하세요.
            
            {
              "has_intent": boolean,
              "complaint_reason": "string",
              "defense_attempted": boolean,
              "defense_success": boolean,
              "defense_actions": ["string", ...],
              "raw_summary": "string"
            }

            분석할 상담 원문: "%s"
            """, rawIssue);

        try {
            log.info("[AI] 호출 모델: {}, 분석 프로세스 시작...", MODEL_NAME);
            
            String rawResponse = restClient.post()
                    .uri(url)
                    .body(buildGeminiPayload(prompt))
                    .retrieve()
                    // 4xx, 5xx 에러 발생 시 상세 메시지 로그 기록
                    .onStatus(HttpStatusCode::isError, (request, response) -> {
                        log.error("[AI API Error] 상태 코드: {}, 사유: {}", response.getStatusCode(), response.getStatusText());
                    })
                    .body(String.class);

            // 1. Gemini 응답 구조에서 실제 텍스트 부분 추출 및 검증
            String extractedText = parseGeminiResponse(rawResponse);
            
            // 2. JSON 문자열 정제 (마크다운 제거 등)
            String cleanedJson = cleanJsonString(extractedText);

            log.info("[AI] 분석 성공 및 JSON 파싱 완료");
            return objectMapper.readValue(cleanedJson, AiExtractionResponse.class);

        } catch (Exception e) {
            log.error("[AI Failure] 상담 분석 중 오류 발생: {}", e.getMessage());
            // 배치가 중단되지 않도록 커스텀 예외로 래핑하여 던집니다.
            throw new RuntimeException("AI 추출 실패: " + e.getMessage(), e);
        }
    }
    

    private Map<String, Object> buildGeminiPayload(String prompt) {
        return Map.of(
            "contents", List.of(
                Map.of("parts", List.of(Map.of("text", prompt)))
            ),
            "generationConfig", Map.of(
                "temperature", 0.1, // 분석의 일관성을 위해 낮은 온도 설정
                "response_mime_type", "application/json"
            )
        );
    }

    private String parseGeminiResponse(String response) throws Exception {
        JsonNode root = objectMapper.readTree(response);
        JsonNode textNode = root.path("candidates")
                .get(0)
                .path("content")
                .path("parts")
                .get(0)
                .path("text");

        if (textNode.isMissingNode() || textNode.asText().isBlank()) {
            throw new RuntimeException("Gemini 응답에서 분석 텍스트를 찾을 수 없습니다.");
        }
        
        return textNode.asText();
    }

    private String cleanJsonString(String raw) {
        if (raw == null || raw.isBlank()) return "{}";
        return raw.replaceAll("(?s)^.*?\\{", "{")  // 처음 등장하는 { 앞의 모든 것 삭제
                  .replaceAll("(?s)\\}.*?$", "}"); // 마지막 등장하는 } 뒤의 모든 것 삭제
    }
}