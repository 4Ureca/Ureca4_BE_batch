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

    private static final String MODEL_NAME = "gemini-2.5-flash-lite"; 
    
    @Retryable(                             
        retryFor = {Exception.class}, 
        maxAttempts = 3, 
        backoff = @Backoff(delay = 2000)
    )
    public AiExtractionResponse extract(String rawIssue, boolean isTerminationMode) {
        if (rawIssue == null || rawIssue.isBlank()) {
            throw new IllegalArgumentException("분석할 상담 원문이 비어 있습니다.");
        }

        String url = "https://generativelanguage.googleapis.com/v1beta/models/" + MODEL_NAME + ":generateContent?key=" + apiKey;
        String summaryFormat = "상담의 전체 흐름을 시간 순서에 따라 '상황 → 조치 → 결과' 형태의 화살표(→)로 연결하여 단 한 문장으로 요약하여 작성.";
        String jsonSchema = isTerminationMode 
            ? """
              {
                "has_intent": boolean,
                "complaint_reason": "string",
                "defense_attempted": boolean,
                "defense_success": boolean,
                "defense_actions": ["string", ...],
                "raw_summary": "string"
              }
              """
            : """
              {
                "raw_summary": "string"
              }
              """;

        String taskInstruction = isTerminationMode 
            ? String.format("상담 분석 후 해지/방어 데이터를 정밀 추출하고, raw_summary는 %s", summaryFormat)
            : String.format("다른 필드 분석은 절대 하지 마세요. 오직 raw_summary 필드만 %s", summaryFormat);

        String prompt = String.format("""
            당신은 상담 분석 전문가입니다.
            [지시사항]: %s

            반드시 아래 JSON 형식으로만 응답하세요. (설명 생략, 다른 필드 추가 금지)
            
            %s

            분석할 상담 원문: "%s"
            """, taskInstruction, jsonSchema, rawIssue);

        try {
            log.info("[AI] 호출 모델: {}, 분석 모드: {}", MODEL_NAME, isTerminationMode ? "해지분석" : "일반요약");
            
            String rawResponse = restClient.post()
                    .uri(url)
                    .body(buildGeminiPayload(prompt))
                    .retrieve()
                    .onStatus(HttpStatusCode::isError, (request, response) -> {
                        log.error("[AI API Error] 상태 코드: {}, 사유: {}", response.getStatusCode(), response.getStatusText());
                    })
                    .body(String.class);

            String extractedText = parseGeminiResponse(rawResponse);
            String cleanedJson = cleanJsonString(extractedText);

            return objectMapper.readValue(cleanedJson, AiExtractionResponse.class);

        } catch (Exception e) {
            log.error("[AI Failure] 상담 분석 중 오류 발생: {}", e.getMessage());
            throw new RuntimeException("AI 추출 실패: " + e.getMessage(), e);
        }
    }
    
    private Map<String, Object> buildGeminiPayload(String prompt) {
        return Map.of(
            "contents", List.of(Map.of("parts", List.of(Map.of("text", prompt)))),
            "generationConfig", Map.of(
                "temperature", 0.1, 
                "response_mime_type", "application/json"
            )
        );
    }

    private String parseGeminiResponse(String response) throws Exception {
        JsonNode root = objectMapper.readTree(response);
        if (root.has("error")) throw new RuntimeException("Gemini API 에러: " + root.path("error").path("message").asText());
        
        JsonNode usage = root.path("usageMetadata");
        if (!usage.isMissingNode()) {
            int inputTokens = usage.path("promptTokenCount").asInt();      
            int totalTokens = usage.path("totalTokenCount").asInt();       
            int calculatedOutput = totalTokens - inputTokens;
            log.info("[AI Usage] 입력: {}, 출력(비용): {}, 총합: {}", 
                     inputTokens, calculatedOutput, totalTokens);
        }

        return root.path("candidates").get(0).path("content").path("parts").get(0).path("text").asText();
    }

    private String cleanJsonString(String raw) {
        if (raw == null || raw.isBlank()) return "{}";
        return raw.replaceAll("(?s)^.*?\\{", "{").replaceAll("(?s)\\}.*?$", "}"); 
    }
}