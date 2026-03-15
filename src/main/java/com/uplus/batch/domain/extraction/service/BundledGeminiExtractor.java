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
import java.util.Set;
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

    // ── complaint_category: 인바운드 고객 불만·문의 유형 ──────────────────────
    // CHN(해지/재약정): COST_HIGH, COST_PENALTY, COMP_BENEFIT, ENV_MOVE, ENV_UNUSED, CONTRACT_END, DUPLICATE_SVC
    // TRB(장애/AS)    : SVC_FAULT, QUAL_SPEED, QUAL_TECH
    // FEE(요금/납부)  : FEE_INQUIRY, ETC_BILLING
    // DEV(기기변경)   : DEVICE_CHANGE
    // ADD(부가서비스) : ADDON_CHG
    private static final String COMPLAINT_CATEGORY_CODES = """
            COST_HIGH     (요금/가격 부담)
            COST_PENALTY  (위약금 부담)
            COMP_BENEFIT  (타사 혜택/서비스 우위)
            ENV_MOVE      (이사/환경 변화로 서비스 불필요)
            ENV_UNUSED    (서비스 미사용/필요 없음)
            CONTRACT_END  (약정 만료 후 해지 원함)
            DUPLICATE_SVC (중복 회선·서비스 정리 목적)
            SVC_FAULT     (서비스 장애·불통)
            QUAL_SPEED    (인터넷·통신 속도 불만)
            QUAL_TECH     (기술 품질·장애 불만)
            ETC_BILLING   (청구·결제 관련 기타 사유)
            FEE_INQUIRY   (요금 내역 문의)
            DEVICE_CHANGE (기기 변경·교체 요청)
            ADDON_CHG     (부가서비스 변경·해지)
            OTHER         (위 분류에 해당하지 않는 기타 사유)""";

    // ── defense_category: 상담사 해지방어 전략 유형 ──────────────────────────
    // 혜택 제공: BNFT_DISCOUNT, BNFT_GIFT, LOYALTY_POINT
    // 요금제 조정: OPT_DOWNGRADE, PLAN_CHANGE, CONTRACT_RENEW
    // 물리 대응: PHYS_RELOCATION, PHYS_TECH_CHECK
    // 행정 처리: ADM_GUIDE, ADM_CLOSE_FAIL
    private static final String DEFENSE_CATEGORY_CODES = """
            BNFT_DISCOUNT   (할인·요금 절감 혜택 제공)
            BNFT_GIFT       (사은품·경품 제공)
            LOYALTY_POINT   (포인트·마일리지 지급)
            OPT_DOWNGRADE   (요금제·옵션 하향 조정)
            PLAN_CHANGE     (고객 상황에 맞는 요금제 변경 제안)
            CONTRACT_RENEW  (재약정 혜택·조건 제시)
            PHYS_RELOCATION (이전 설치 지원)
            PHYS_TECH_CHECK (기술 점검·현장 출동)
            ADM_GUIDE       (안내·상담 후 자발 유지)
            ADM_CLOSE_FAIL  (해지·이탈 처리, 방어 실패)
            OTHER           (위 분류에 해당하지 않는 기타 대응)""";

    private static final Set<String> VALID_COMPLAINT_CATEGORIES = Set.of(
            "COST_HIGH", "COST_PENALTY", "COMP_BENEFIT", "ENV_MOVE", "ENV_UNUSED",
            "CONTRACT_END", "DUPLICATE_SVC", "SVC_FAULT", "QUAL_SPEED", "QUAL_TECH",
            "ETC_BILLING", "FEE_INQUIRY", "DEVICE_CHANGE", "ADDON_CHG", "OTHER");

    private static final Set<String> VALID_DEFENSE_CATEGORIES = Set.of(
            "BNFT_DISCOUNT", "BNFT_GIFT", "LOYALTY_POINT",
            "OPT_DOWNGRADE", "PLAN_CHANGE", "CONTRACT_RENEW",
            "PHYS_RELOCATION", "PHYS_TECH_CHECK",
            "ADM_GUIDE", "ADM_CLOSE_FAIL", "OTHER");

    private static final Set<String> VALID_OUTBOUND_CATEGORIES = Set.of(
            "COST", "NO_NEED", "SWITCH", "CONSIDER", "DISSATISFIED", "OTHER");

    // ── outbound_category: 아웃바운드 거절 사유 유형 ─────────────────────────
    // CONVERTED(전환 성공)는 null. REJECTED 시 아래 코드 중 1개 선택.
    private static final String OUTBOUND_CATEGORY_CODES = """
            COST        (요금·비용 부담으로 거절)
            NO_NEED     (서비스 필요 없음·이사 등 환경 변화로 거절)
            SWITCH      (타사 전환 의사로 거절)
            CONSIDER    (아직 고려 중, 결정 유보로 거절)
            DISSATISFIED(서비스 품질·기술 불만으로 거절)
            OTHER       (기타 사유로 거절)""";

    private String buildPrompt(List<BundleItem> items) {
        boolean isOutbound = items.stream().anyMatch(it -> "OUTBOUND".equals(it.consultationType()));
        return isOutbound ? buildOutboundPrompt(items) : buildInboundPrompt(items);
    }

    /**
     * 인바운드 전용 프롬프트.
     * CHN 카테고리: has_intent·defense_* 필드 정밀 추출.
     * 비CHN 카테고리: has_intent=false, defense_* 기본값.
     * 전체 인바운드: complaint_category·defense_category 반드시 코드 매핑 (없으면 OTHER).
     */
    private String buildInboundPrompt(List<BundleItem> items) {
        StringBuilder sb = new StringBuilder();
        sb.append("""
                당신은 통신사 상담 분석 전문가입니다.
                아래 인바운드 상담 원문 목록을 분석하여 각 건의 결과를 JSON 배열로 반환하세요.

                [응답 형식] 반드시 아래 JSON 배열만 출력하세요 (마크다운 코드블록, 설명 없이):
                [
                  {
                    "index": 숫자,
                    "has_intent": boolean,
                    "complaint_reason": "string 또는 null",
                    "complaint_category": "코드 또는 null",
                    "defense_attempted": boolean,
                    "defense_success": boolean,
                    "defense_actions": ["string", ...],
                    "defense_category": "코드",
                    "raw_summary": "string",
                    "outbound_call_result": null,
                    "outbound_report": null,
                    "outbound_category": null
                  }
                ]

                [필드 지시사항]:
                - raw_summary: '상황 → 조치 → 결과' 형태 단 한 문장 (필수)
                - complaint_reason: 고객의 주된 불만/문의 사유를 핵심만 담은 간결한 명사구 (예: "중도 해지 시 위약금 부담", "타사 대비 높은 요금")
                - defense_actions: 상담사가 취한 방어 조치를 2~4개의 짧은 명사구 배열 (예: ["위약금 안내", "요금제 변경 제안", "재약정 제안"])
                - CHN(해지/재약정) 카테고리: has_intent · defense_* 필드 정밀 추출
                - 비CHN 카테고리: has_intent=false, defense_attempted=false, defense_success=false, defense_actions=[], complaint_reason=null
                - complaint_category: 모든 인바운드 상담에서 아래 코드 중 가장 알맞은 1개를 반드시 선택 (해당 없으면 OTHER)
                """)
                .append(COMPLAINT_CATEGORY_CODES)
                .append("""

                - defense_category: 모든 인바운드 상담에서 아래 코드 중 가장 알맞은 1개를 반드시 선택 (해당 없으면 OTHER)
                """)
                .append(DEFENSE_CATEGORY_CODES)
                .append("""

                - outbound_call_result·outbound_report·outbound_category: 인바운드이므로 항상 null

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

    /**
     * 아웃바운드 전용 프롬프트.
     * 상담사가 이탈 위험 고객에게 먼저 전화를 건 해지방어 상담이므로
     * has_intent=true, defense_attempted=true 확정.
     * AI는 통화 결과(CONVERTED/REJECTED), 이탈 사유, 결과 보고서를 추출한다.
     */
    private String buildOutboundPrompt(List<BundleItem> items) {
        StringBuilder sb = new StringBuilder();
        sb.append("""
                당신은 통신사 상담 분석 전문가입니다.
                아래 아웃바운드 상담 원문 목록을 분석하여 각 건의 결과를 JSON 배열로 반환하세요.
                아웃바운드 상담은 상담사가 이탈 위험 고객에게 먼저 전화를 건 해지방어 상담입니다.

                [응답 형식] 반드시 아래 JSON 배열만 출력하세요 (마크다운 코드블록, 설명 없이):
                [
                  {
                    "index": 숫자,
                    "has_intent": true,
                    "complaint_reason": null,
                    "complaint_category": null,
                    "defense_attempted": true,
                    "defense_success": boolean,
                    "defense_actions": [],
                    "defense_category": null,
                    "raw_summary": "string",
                    "outbound_call_result": "CONVERTED 또는 REJECTED",
                    "outbound_report": "string",
                    "outbound_category": "REJECTED 시 반드시 코드 입력, CONVERTED 시 null"
                  }
                ]

                [필드 지시사항]:
                - raw_summary: '상황 → 조치 → 결과' 형태 단 한 문장 (필수)
                - has_intent: 항상 true (이탈 위험 고객 확정)
                - defense_attempted: 항상 true (전화 목적 자체가 방어 시도)
                - defense_success: 고객이 최종 유지·재약정하면 true, 여전히 해지 의사면 false
                - outbound_call_result: 고객이 최종 유지·재약정하면 "CONVERTED", 거절하면 "REJECTED"
                - outbound_report: 핵심만 담은 1~2문장 간결 요약 (예: "요금 절감 혜택 제안, 고객이 타사 전환 의사 고수하여 해지 처리", "재약정 혜택 안내 후 고객 유지 성공")
                - outbound_category: REJECTED인 경우 아래 코드 중 반드시 1개 선택 (해당 없으면 OTHER). CONVERTED면 null
                """)
                .append(OUTBOUND_CATEGORY_CODES)
                .append("""

                - complaint_reason·complaint_category·defense_actions·defense_category: 항상 null 또는 []

                [상담 목록]:
                """);

        for (int i = 0; i < items.size(); i++) {
            BundleItem item = items.get(i);
            sb.append(String.format("\n[%d] (카테고리: %s)\n%s\n",
                    i, item.categoryCode(), item.rawTextJson()));
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
            boolean isInbound = "INBOUND".equals(item.consultationType());
            String complaintCat = toValidCode(node.path("complaint_category"), VALID_COMPLAINT_CATEGORIES, isInbound);
            String defenseCat   = toValidCode(node.path("defense_category"),   VALID_DEFENSE_CATEGORIES,   isInbound);
            String outboundCallResult = node.path("outbound_call_result").isNull()
                    ? null : node.path("outbound_call_result").asText(null);
            String outboundReport = node.path("outbound_report").isNull()
                    ? null : node.path("outbound_report").asText(null);
            String outboundCat = !isInbound
                    ? toValidOutboundCode(node.path("outbound_category"), outboundCallResult)
                    : null;

            results.add(new BundledAiResult(
                    idx,
                    item.consultId(),
                    item.categoryCode(),
                    node.path("has_intent").asBoolean(false),
                    reason,
                    node.path("defense_attempted").asBoolean(false),
                    node.path("defense_success").asBoolean(false),
                    Collections.unmodifiableList(actions),
                    node.path("raw_summary").asText(""),
                    complaintCat,
                    defenseCat,
                    outboundCallResult,
                    outboundReport,
                    outboundCat
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
                            false, null, false, false, List.of(), "[AI 분석 누락]", null, null, null, null, null));
                }
            }
        }

        return results;
    }

    /**
     * AI 응답 코드를 유효 코드 집합으로 검증한다.
     * - null/빈값이고 인바운드면 "OTHER", 아웃바운드면 null 반환
     * - 유효하지 않은 코드값이 들어오면 "OTHER"(인바운드) 또는 null(아웃바운드) fallback
     */
    private String toValidCode(JsonNode node, Set<String> validCodes, boolean isInbound) {
        if (node.isNull() || node.isMissingNode()) {
            return isInbound ? "OTHER" : null;
        }
        String code = node.asText(null);
        if (code == null || code.isBlank()) {
            return isInbound ? "OTHER" : null;
        }
        if (!validCodes.contains(code)) {
            log.warn("[BundledAI] 알 수 없는 카테고리 코드 '{}' → {} fallback", code, isInbound ? "OTHER" : "null");
            return isInbound ? "OTHER" : null;
        }
        return code;
    }

    /**
     * 아웃바운드 outbound_category 코드를 검증한다.
     * - CONVERTED이면 항상 null
     * - REJECTED인데 코드가 없거나 유효하지 않으면 "OTHER" fallback
     */
    private String toValidOutboundCode(JsonNode node, String outboundCallResult) {
        if (!"REJECTED".equals(outboundCallResult)) return null;
        if (node.isNull() || node.isMissingNode()) return "OTHER";
        String code = node.asText(null);
        if (code == null || code.isBlank()) return "OTHER";
        if (!VALID_OUTBOUND_CATEGORIES.contains(code)) {
            log.warn("[BundledAI] 알 수 없는 outbound_category 코드 '{}' → OTHER fallback", code);
            return "OTHER";
        }
        return code;
    }

    private String extractJsonArray(String raw) {
        if (raw == null || raw.isBlank()) return "[]";
        int start = raw.indexOf('[');
        int end   = raw.lastIndexOf(']');
        if (start < 0 || end < 0 || end <= start) return "[]";
        return raw.substring(start, end + 1);
    }

    // ─── 입력 DTO ─────────────────────────────────────────────────────────────

    public record BundleItem(long consultId, String categoryCode, String rawTextJson, String consultationType) {}
}
