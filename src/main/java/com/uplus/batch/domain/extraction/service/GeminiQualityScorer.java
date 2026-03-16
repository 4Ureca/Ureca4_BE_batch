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
import com.uplus.batch.domain.extraction.dto.QualityScoringResponse; // 신규 DTO
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class GeminiQualityScorer {

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
    	    backoff = @Backoff(
    	        delay = 2000,      
    	        multiplier = 2.0,  
    	        maxDelay = 10000   
    	    )
    	)
    public QualityScoringResponse evaluate(String rawIssue, String manualContent) {
        if (rawIssue == null || rawIssue.isBlank()) {
            throw new IllegalArgumentException("분석할 상담 원문이 비어 있습니다.");
        }
        if (manualContent == null || manualContent.isBlank()) {
            throw new IllegalArgumentException("채점 기준(매뉴얼)이 비어 있습니다.");
        }

        String url = "https://generativelanguage.googleapis.com/v1beta/models/" + MODEL_NAME + ":generateContent?key=" + apiKey;

        // [Prompt] 우수 상담 채점을 위한 전문 프롬프트 구성
        String prompt = String.format("""
            당신은 상담 품질 관리(QA) 전문가입니다. 
            제공된 [채점 기준]을 바탕으로 [상담 원문]의 점수를 매기고 분석 결과를 작성하세요.

            [채점 기준(매뉴얼)]:
            %s

            [상담 원문]:
            %s

            [지시사항]:
            1. 점수는 0점부터 100점 사이의 정수로 부여하세요.
            2. 매뉴얼의 필수 응대 요소(인사, 경청, 정확한 정보 제공 등) 준수 여부를 정밀하게 체크하세요.
            3. 점수가 90점 이상인 경우에만 'is_candidate' 필드를 true로 설정하세요.
            4. evaluation_reason에는 왜 해당 점수를 주었는지 장단점을 포함하여 구체적으로 작성하세요.
            
            반드시 아래 JSON 형식으로만 응답하세요. (설명 생략)
            {
              "score": number,
              "evaluation_reason": "string",
              "is_candidate": boolean
            }
            """, manualContent, rawIssue);

        try {
            log.info("[AI Quality Scorer] 호출 모델: {}, 채점 시작", MODEL_NAME);
            
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

            return objectMapper.readValue(cleanedJson, QualityScoringResponse.class);

        } catch (Exception e) {
            log.error("[AI Failure] 상담 채점 중 오류 발생: {}", e.getMessage());
            throw new RuntimeException("AI 채점 실패: " + e.getMessage(), e);
        }
    }
    
    private Map<String, Object> buildGeminiPayload(String prompt) {
        return Map.of(
            "contents", List.of(Map.of("parts", List.of(Map.of("text", prompt)))),
            "generationConfig", Map.of(
                "temperature", 0.1, // 채점의 일관성을 위해 낮은 온도로 설정
                "response_mime_type", "application/json"
            )
        );
    }

    private String parseGeminiResponse(String response) throws Exception {
        JsonNode root = objectMapper.readTree(response);
        if (root.has("error")) throw new RuntimeException("Gemini API 에러: " + root.path("error").path("message").asText());
        
        JsonNode usage = root.path("usageMetadata");
        if (!usage.isMissingNode()) {
            log.info("[AI Usage] 입력: {}, 출력: {}, 총합: {}", 
                     usage.path("promptTokenCount").asInt(), 
                     usage.path("candidatesTokenCount").asInt(), 
                     usage.path("totalTokenCount").asInt());
        }

        return root.path("candidates").get(0).path("content").path("parts").get(0).path("text").asText();
    }

    private String cleanJsonString(String raw) {
        if (raw == null || raw.isBlank()) return "{}";
        return raw.replaceAll("(?s)^.*?\\{", "{").replaceAll("(?s)\\}.*?$", "}"); 
    }
}