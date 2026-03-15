package com.uplus.batch.domain.extraction.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.extraction.dto.BundledAiResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * N건의 상담 원문을 하나의 Gemini API 호출로 처리하는 번들 추출기.
 *
 * <h3>비용 최적화</h3>
 * <ul>
 *   <li>기존 GeminiExtractor: 1건 = 1 API 호출</li>
 *   <li>이 클래스: N건 = 1 API 호출 → N배 비용 절감</li>
 * </ul>
 *
 * <h3>Rate Limit 대응</h3>
 * <ul>
 *   <li>429 응답 시 지수 백오프(Exponential Backoff) 자동 재시도</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BundledGeminiExtractor {

    @Value("${gemini.api.key}")
    private String apiKey;

    private final ObjectMapper objectMapper;

    private static final String MODEL_NAME = "gemini-2.5-flash-lite";
    private static final String API_URL =
            "https://generativelanguage.googleapis.com/v1beta/models/" + MODEL_NAME + ":generateContent?key=";

    private final RestClient restClient = RestClient.builder()
            .requestFactory(new org.springframework.http.client.SimpleClientHttpRequestFactory() {{
                setConnectTimeout(10_000);
                setReadTimeout(120_000); // 번들 처리를 고려해 2분
            }})
            .build();

    /**
     * N건의 상담을 하나의 API 호출로 분석한다.
     *
     * @param items         분석 대상 목록 (consultId, categoryCode, rawTextJson)
     * @param maxRetries    최대 재시도 횟수
     * @param retryBaseMs   재시도 초기 대기시간(ms), 지수 백오프 적용
     * @return 각 건에 대한 AI 분석 결과 목록
     */
    public List<BundledAiResult> extractBatch(List<BundleItem> items, int maxRetries, long retryBaseMs) {
        if (items == null || items.isEmpty()) return List.of();

        String prompt = buildPrompt(items);
        String url    = API_URL + apiKey;

        int  attempts = 0;
        long delay    = retryBaseMs;

        while (true) {
            try {
                log.info("[BundledAI] {}건 번들 호출 (attempt {}/{})", items.size(), attempts + 1, maxRetries);

                String rawResponse = restClient.post()
                        .uri(url)
                        .body(buildPayload(prompt))
                        .retrieve()
                        .onStatus(HttpStatusCode::isError, (req, resp) -> {
                            int code = resp.getStatusCode().value();
                            if (code == 429) log.warn("[BundledAI] Rate limit(429) 감지 — 재시도 예정");
                            else             log.error("[BundledAI] API 오류 {}", code);
                        })
                        .body(String.class);

                List<BundledAiResult> results = parseResponse(rawResponse, items);
                log.info("[BundledAI] 완료 — {}건 파싱 성공", results.size());
                return results;

            } catch (Exception e) {
                attempts++;
                if (attempts >= maxRetries) {
                    log.error("[BundledAI] 최대 재시도({}) 초과: {}", maxRetries, e.getMessage());
                    throw new RuntimeException("번들 AI 호출 최종 실패: " + e.getMessage(), e);
                }
                log.warn("[BundledAI] 재시도 {}/{}, {}ms 대기: {}", attempts, maxRetries, delay, e.getMessage());
                try { Thread.sleep(delay); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("재시도 대기 중 인터럽트", ie);
                }
                delay = Math.min(delay * 2, 60_000); // 지수 백오프, 최대 60초
            }
        }
    }

    // ─── Prompt 구성 ──────────────────────────────────────────────────────────

    private String buildPrompt(List<BundleItem> items) {
        StringBuilder sb = new StringBuilder();
        sb.append("""
                당신은 통신사 상담 분석 전문가입니다.
                아래 상담 원문 목록을 분석하여 각 건의 결과를 JSON 배열로 반환하세요.

                [응답 형식] 반드시 아래 JSON 배열만 출력하세요 (마크다운 코드블록, 설명 없이):
                [
                  {
                    "index": 숫자,
                    "has_intent": boolean,
                    "complaint_reason": "string 또는 null",
                    "defense_attempted": boolean,
                    "defense_success": boolean,
                    "defense_actions": ["string", ...],
                    "raw_summary": "string"
                  }
                ]

                [필드 지시사항]:
                - raw_summary: '상황 → 조치 → 결과' 형태 단 한 문장 (필수)
                - CHN(해지/재약정) 카테고리: has_intent · defense_* 필드 정밀 추출
                - 비CHN 카테고리: has_intent=false, defense_attempted=false, defense_success=false, defense_actions=[], complaint_reason=null

                [상담 목록]:
                """);

        for (int i = 0; i < items.size(); i++) {
            BundleItem item = items.get(i);
            boolean isChn = item.categoryCode() != null && item.categoryCode().contains("CHN");
            sb.append(String.format("\n[%d] (카테고리: %s | 해지분석: %s)\n%s\n",
                    i, item.categoryCode(), isChn ? "필요" : "불필요", item.rawTextJson()));
        }
        return sb.toString();
    }

    private Map<String, Object> buildPayload(String prompt) {
        return Map.of(
                "contents", List.of(Map.of("parts", List.of(Map.of("text", prompt)))),
                "generationConfig", Map.of("temperature", 0.1, "response_mime_type", "application/json")
        );
    }

    // ─── 응답 파싱 ────────────────────────────────────────────────────────────

    private List<BundledAiResult> parseResponse(String rawResponse, List<BundleItem> items) throws Exception {
        JsonNode root = objectMapper.readTree(rawResponse);

        if (root.has("error")) {
            throw new RuntimeException("Gemini API 오류: " + root.path("error").path("message").asText());
        }

        JsonNode usage = root.path("usageMetadata");
        if (!usage.isMissingNode()) {
            log.info("[BundledAI] 입력토큰={}, 출력토큰={}, 합계={}",
                    usage.path("promptTokenCount").asInt(),
                    usage.path("candidatesTokenCount").asInt(),
                    usage.path("totalTokenCount").asInt());
        }

        String text   = root.path("candidates").get(0).path("content").path("parts").get(0).path("text").asText();
        String jsonArr = extractJsonArray(text);
        JsonNode arr   = objectMapper.readTree(jsonArr);

        List<BundledAiResult> results = new ArrayList<>(items.size());

        for (JsonNode node : arr) {
            int idx = node.path("index").asInt(-1);
            if (idx < 0 || idx >= items.size()) continue;

            BundleItem item = items.get(idx);

            List<String> actions = new ArrayList<>();
            for (JsonNode a : node.path("defense_actions")) actions.add(a.asText());

            String reason = node.path("complaint_reason").isNull()
                    ? null : node.path("complaint_reason").asText(null);

            results.add(new BundledAiResult(
                    idx,
                    item.consultId(),
                    item.categoryCode(),
                    node.path("has_intent").asBoolean(false),
                    reason,
                    node.path("defense_attempted").asBoolean(false),
                    node.path("defense_success").asBoolean(false),
                    Collections.unmodifiableList(actions),
                    node.path("raw_summary").asText("")
            ));
        }

        // AI가 일부 항목을 빠뜨린 경우 fallback 보완
        if (results.size() < items.size()) {
            log.warn("[BundledAI] 응답 항목 수({})가 요청 수({})보다 적음 — fallback 보완",
                    results.size(), items.size());
            var returnedIndices = results.stream()
                    .map(BundledAiResult::index)
                    .collect(Collectors.toSet());
            for (int i = 0; i < items.size(); i++) {
                if (!returnedIndices.contains(i)) {
                    BundleItem item = items.get(i);
                    results.add(new BundledAiResult(i, item.consultId(), item.categoryCode(),
                            false, null, false, false, List.of(), "[AI 분석 누락]"));
                }
            }
        }

        return results;
    }

    private String extractJsonArray(String raw) {
        if (raw == null || raw.isBlank()) return "[]";
        int start = raw.indexOf('[');
        int end   = raw.lastIndexOf(']');
        if (start < 0 || end < 0 || end <= start) return "[]";
        return raw.substring(start, end + 1);
    }

    // ─── 입력 DTO ─────────────────────────────────────────────────────────────

    public record BundleItem(long consultId, String categoryCode, String rawTextJson) {}
}
