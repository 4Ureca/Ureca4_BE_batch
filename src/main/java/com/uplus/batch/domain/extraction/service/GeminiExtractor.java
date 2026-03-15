package com.uplus.batch.domain.extraction.service;

import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import com.fasterxml.jackson.core.type.TypeReference;
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
            .requestFactory(new SimpleClientHttpRequestFactory() {{
                setConnectTimeout(10000);
                setReadTimeout(180000); // 번들 처리를 위해 3분으로 확장
            }})
            .build();

    private static final String MODEL_NAME = "gemini-2.5-flash-lite";

    /**
     * [번들 모드] 여러 건의 상담을 하나의 API 호출로 묶어서 분석
     * @param rawIssues 상담 대화 원문 리스트
     * @param groupType 분석 모드 (OUTBOUND / INBOUND_CHN / INBOUND_NORMAL)
     * @param validCodes DB에서 조회한 유효 코드 목록
     */
    @Retryable(retryFor = {Exception.class}, maxAttempts = 3, backoff = @Backoff(delay = 2000))
    public List<AiExtractionResponse> extractBatch(List<String> rawIssues, String groupType, Map<String, String> validCodes) {
        if (rawIssues == null || rawIssues.isEmpty()) return List.of();

        // 1. 그룹 성격에 따른 번들 프롬프트 생성
        String prompt;
        if ("OUTBOUND".equals(groupType)) {
            prompt = buildOutboundBatchPrompt(rawIssues, validCodes.get("outbound_category"));
        } else if ("INBOUND_CHN".equals(groupType)) {
            prompt = buildChnBatchPrompt(rawIssues, validCodes.get("complaint_category"), validCodes.get("defense_category"));
        } else {
            prompt = buildNormalBatchPrompt(rawIssues);
        }

        try {
            String url = "https://generativelanguage.googleapis.com/v1beta/models/" + MODEL_NAME + ":generateContent?key=" + apiKey;
            
            log.info("[AI Bundle] 분석 요청 - 모드: {}, 건수: {}", groupType, rawIssues.size());

            String rawResponse = restClient.post()
                    .uri(url)
                    .body(Map.of("contents", List.of(Map.of("parts", List.of(Map.of("text", prompt)))),
                               "generationConfig", Map.of("temperature", 0.1, "response_mime_type", "application/json")))
                    .retrieve()
                    .body(String.class);

            String extractedText = parseGeminiResponse(rawResponse);
            // JSON 배열을 List<AiExtractionResponse>로 파싱
            return objectMapper.readValue(cleanJsonString(extractedText), new TypeReference<List<AiExtractionResponse>>() {});

        } catch (Exception e) {
            log.error("[AI Bundle Failure] 번들 분석 중 오류: {}", e.getMessage());
            throw new RuntimeException("AI 번들 추출 실패: " + e.getMessage(), e);
        }
    }

    /**
     * [모드 1] 아웃바운드 번들 프롬프트
     */
    private String buildOutboundBatchPrompt(List<String> issues, String outCodes) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("""
            당신은 통신사 아웃바운드(이탈방어 권유) 상담 분석 전문가입니다.
            아래 %d건의 리스트를 분석하여 JSON 배열로 응답하세요.

            [분류 지침]: 
            제공된 코드는 '코드명(의미/설명)' 형식입니다. 설명을 읽고 가장 적합한 '코드명'만 결과값에 사용하세요.
            - 거절 사유 코드 목록: %s

            [응답 형식]: 반드시 아래 필드만 포함한 JSON 객체들의 '배열'([])로 응답하세요. (순서 엄수)
            {
              "raw_summary": "상황 → 조치 → 결과 형태의 단 한 문장 요약",
              "outbound_call_result": "고객 유지 시 CONVERTED, 거절 시 REJECTED",
              "outbound_report": "결과에 대한 핵심 요약 1~2문장",
              "outbound_category": "REJECTED인 경우 위 목록 중 '코드명' 하나만 선택 (CONVERTED면 null)"
            }
            
            [상담 목록]:
            """, issues.size(), outCodes));

        for (int i = 0; i < issues.size(); i++) {
            sb.append(String.format("\n[%d] 원문: \"%s\"\n", i, issues.get(i)));
        }
        return sb.toString();
    }

    /**
     * [모드 2] 인바운드 해지방어 번들 프롬프트
     */
    private String buildChnBatchPrompt(List<String> issues, String compCodes, String defCodes) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("""
            당신은 통신사 해지방어 상담 분석 전문가입니다. 고객의 해지 문의 과정을 분석하여 아래 %d건의 리스트를 JSON 배열로 응답하세요.

            [분류 지침]:
            아래 카테고리 목록은 '코드명(상세 설명)'으로 구성되어 있습니다. 상세 설명을 바탕으로 문맥을 정확히 파악하되, JSON 결과값에는 괄호를 제외한 '코드명'만 입력하세요.
            - 고객 불만 카테고리: %s
            - 상담사 방어 카테고리: %s

            [응답 형식]: 반드시 아래 필드만 포함한 JSON 객체들의 '배열'([])로 응답하세요. (순서 엄수)
            {
              "raw_summary": "상황 → 조치 → 결과 형태의 단 한 문장 요약",
              "has_intent": 고객의 해지/이탈 의사 존재 여부 (boolean),
              "complaint_reason": "고객의 주된 불만 사유 핵심 요약",
              "complaint_category": "위 목록 중 가장 알맞은 '코드명' 1개 선택",
              "defense_attempted": 상담사의 방어 시도 여부 (boolean),
              "defense_success": 최종 방어 성공 여부 (boolean),
              "defense_actions": ["방어 조치 2~4개를 짧은 명사구 배열"],
              "defense_category": "위 목록 중 가장 알맞은 '코드명' 1개 선택"
            }

            [상담 목록]:
            """, issues.size(), compCodes, defCodes));

        for (int i = 0; i < issues.size(); i++) {
            sb.append(String.format("\n[%d] 원문: \"%s\"\n", i, issues.get(i)));
        }
        return sb.toString();
    }

    /**
     * [모드 3] 일반 인바운드 번들 프롬프트
     */
    private String buildNormalBatchPrompt(List<String> issues) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("""
            당신은 상담 요약 전문가입니다. 아래 %d건의 상담 리스트를 분석하여 JSON 배열로 응답하세요.

            [응답 형식]: 반드시 아래 필드만 포함한 JSON 객체들의 '배열'([])로 응답하세요. (순서 엄수)
            {
              "raw_summary": "상황 → 조치 → 결과 형태의 단 한 문장 요약"
            }

            [상담 목록]:
            """, issues.size()));

        for (int i = 0; i < issues.size(); i++) {
            sb.append(String.format("\n[%d] 원문: \"%s\"\n", i, issues.get(i)));
        }
        return sb.toString();
    }

    private String parseGeminiResponse(String response) throws Exception {
        JsonNode root = objectMapper.readTree(response);
        if (root.has("error")) throw new RuntimeException("Gemini API Error: " + root.path("error").path("message").asText());
        return root.path("candidates").get(0).path("content").path("parts").get(0).path("text").asText();
    }

    private String cleanJsonString(String raw) {
        if (raw == null || raw.isBlank()) return "[]";
        
        // '[' 또는 '{'로 시작하는 지점부터 ']' 또는 '}'로 끝나는 지점까지 추출
        int start = Math.min(
            raw.indexOf('[') != -1 ? raw.indexOf('[') : Integer.MAX_VALUE,
            raw.indexOf('{') != -1 ? raw.indexOf('{') : Integer.MAX_VALUE
        );
        int end = Math.max(raw.lastIndexOf(']'), raw.lastIndexOf('}'));

        if (start == Integer.MAX_VALUE || end == -1 || start >= end) return "[]";
        
        return raw.substring(start, end + 1);
    }
}