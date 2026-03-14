package com.uplus.batch.synthetic;

import com.uplus.batch.domain.summary.entity.SummaryEventStatus;
import com.uplus.batch.domain.summary.repository.SummaryEventStatusRepository;
import com.uplus.batch.synthetic.OutboundRawTextGenerator.OutboundTextResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 아웃바운드 합성 상담 데이터 생성 팩토리.
 *
 * <p>아웃바운드 상담은 상담사가 고객에게 먼저 전화를 거는 방식으로,
 * 해지 방어·재약정 유도·서비스 업그레이드 시나리오를 생성한다.
 *
 * <p>인바운드 {@link SyntheticConsultationFactory}와의 차이점:
 * <ul>
 *   <li>채널: 항상 CALL (아웃바운드는 전화 방식)</li>
 *   <li>카테고리: 항상 CHN (해지방어·재약정 관련)</li>
 *   <li>원문: 상담사가 먼저 전화하는 아웃바운드 형식 ({@link OutboundRawTextGenerator})</li>
 *   <li>retention_analysis: AI가 원문 분석 후 채움 (인바운드와 동일한 파이프라인)</li>
 *   <li>AI 트리거: result_event_status(OUTBOUND)·summary_event_status REQUESTED 생성</li>
 * </ul>
 *
 * <p>§2 분포 조건:
 * <ul>
 *   <li>결과: CONVERTED 45% / REJECTED 55% (outbound_category 분포로 제어)</li>
 *   <li>고객 만족도 평가: 40% (아웃바운드 특성상 인바운드 70%보다 낮음)</li>
 *   <li>위험 감지 로그: 항상 생성 (아웃바운드 대상 = 이탈 위험 고객)</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboundConsultationFactory {

    private final SyntheticPersonMatcher personMatcher;
    private final OutboundRawTextGenerator outboundRawTextGenerator;
    private final JdbcTemplate jdbcTemplate;
    private final SummaryEventStatusRepository summaryEventStatusRepo;

    public record BatchResult(List<Long> consultIds, List<String> categoryCodes) {
        public boolean isEmpty() { return consultIds.isEmpty(); }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Step 1: 아웃바운드 consultation_results + raw_texts
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 아웃바운드 상담 결과서·원문·분석 더미 데이터를 하나의 트랜잭션으로 삽입한다.
     *
     * <p>AI 추출 트리거(result_event_status)·요약 트리거(summary_event_status)는
     * 생성하지 않는다. 아웃바운드 데이터의 AI 분석은 별도 배치에서 처리한다.
     */
    @Transactional
    public BatchResult executeStep1(int batchSize) {
        return executeStep1WithDate(batchSize, null);
    }

    /**
     * 과거 데이터 생성용 오버로드 — targetDate가 지정되면 해당 날짜의 랜덤 업무시간으로 created_at을 설정한다.
     */
    @Transactional
    public BatchResult executeStep1WithDate(int batchSize, LocalDate targetDate) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        var agents    = personMatcher.getAgents();
        var customers = personMatcher.getCustomers();
        var chnCodes  = personMatcher.getChnCodes();
        var feeCodes  = personMatcher.getFeeCodes();
        var trbCodes  = personMatcher.getTrbCodes();

        if (agents.isEmpty() || customers.isEmpty() || chnCodes.isEmpty()) {
            log.warn("[OutboundFactory] 상담사·고객·CHN 카테고리 없음 — Step1 스킵");
            return new BatchResult(List.of(), List.of());
        }

        String resultSql = """
                INSERT INTO consultation_results
                    (emp_id, customer_id, channel, category_code, duration_sec,
                     iam_issue, iam_action, iam_memo, consultation_type, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OUTBOUND', ?)
                """;

        List<Long>    consultIds    = new ArrayList<>(batchSize);
        List<String>  categoryCodes = new ArrayList<>(batchSize);
        List<Integer> empIds        = new ArrayList<>(batchSize);
        List<Long>    customerIds   = new ArrayList<>(batchSize);

        List<Object[]> rawArgs = new ArrayList<>(batchSize);

        for (int i = 0; i < batchSize; i++) {
            var agent    = agents.get(random.nextInt(agents.size()));
            var customer = customers.get(random.nextInt(customers.size()));
            String categoryType = pickCategoryType(random.nextInt(100)); // CHN 60% / FEE 25% / TRB 15%
            List<String> codePool = switch (categoryType) {
                case "FEE" -> feeCodes.isEmpty() ? chnCodes : feeCodes;
                case "TRB" -> trbCodes.isEmpty() ? chnCodes : trbCodes;
                default    -> chnCodes;
            };
            String categoryCode = codePool.get(random.nextInt(codePool.size()));
            int durationSec = 120 + random.nextInt(481); // CALL: 120~600s

            String outboundCategoryKey = pickOutboundCategoryKey(categoryType, random.nextInt(100));
            OutboundTextResult textResult = outboundRawTextGenerator.generate(
                    categoryType, outboundCategoryKey, new Random(ThreadLocalRandom.current().nextLong()));

            final int    empId   = agent.empId();
            final long   custId  = customer.customerId();
            final String catCode = categoryCode;
            final int    dur     = durationSec;
            final String issue   = textResult.iamIssue();
            final String action  = textResult.iamAction();
            final String memo    = buildMemo(textResult);
            final LocalDateTime now = (targetDate != null)
                    ? randomBusinessTime(targetDate, random)
                    : LocalDateTime.now();

            KeyHolder keyHolder = new GeneratedKeyHolder();
            jdbcTemplate.update(con -> {
                PreparedStatement ps = con.prepareStatement(resultSql, Statement.RETURN_GENERATED_KEYS);
                ps.setInt(1, empId);
                ps.setLong(2, custId);
                ps.setString(3, "CALL");     // 아웃바운드는 항상 CALL
                ps.setString(4, catCode);
                ps.setInt(5, dur);
                ps.setString(6, issue);
                ps.setString(7, action);
                ps.setString(8, memo);
                ps.setObject(9, now);
                return ps;
            }, keyHolder);

            long consultId = keyHolder.getKey().longValue();
            consultIds.add(consultId);
            categoryCodes.add(categoryCode);
            empIds.add(empId);
            customerIds.add(custId);
            rawArgs.add(new Object[]{consultId, textResult.rawTextJson()});
        }

        // ── consultation_raw_texts Bulk INSERT ──────────────────────────────
        jdbcTemplate.batchUpdate(
                "INSERT INTO consultation_raw_texts (consult_id, raw_text_json) VALUES (?, ?)",
                rawArgs
        );

        // ── 연관 테이블 Bulk INSERT ──────────────────────────────────────────
        insertClientReviews(consultIds, random);
        insertCustomerRiskLogs(consultIds, empIds, customerIds, random);

        log.info("[OutboundFactory] Step1 완료 — {}건 생성 | consultId 범위: {} ~ {}",
                batchSize, consultIds.get(0), consultIds.get(consultIds.size() - 1));

        return new BatchResult(consultIds, categoryCodes);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Step 2: result_event_status REQUESTED (OUTBOUND, Step1 커밋 후)
    // ─────────────────────────────────────────────────────────────────────────

    public void triggerAiExtraction(List<Long> consultIds, List<String> categoryCodes) {
        List<Object[]> args = new ArrayList<>(consultIds.size());
        for (int i = 0; i < consultIds.size(); i++) {
            args.add(new Object[]{consultIds.get(i), categoryCodes.get(i)});
        }
        jdbcTemplate.batchUpdate(
                """
                INSERT INTO result_event_status
                    (consult_id, category_code, consultation_type, status, retry_count, created_at, updated_at)
                VALUES (?, ?, 'OUTBOUND', 'REQUESTED', 0, NOW(), NOW())
                """,
                args
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Step 3: summary_event_status REQUESTED (Step1 커밋 후)
    // ─────────────────────────────────────────────────────────────────────────

    public void triggerSummaryGeneration(List<Long> consultIds) {
        List<SummaryEventStatus> events = new ArrayList<>(consultIds.size());
        for (Long cid : consultIds) {
            events.add(SummaryEventStatus.builder().consultId(cid).build());
        }
        summaryEventStatusRepo.saveAll(events);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  연관 테이블 INSERT 헬퍼
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * client_review — 고객 만족도 평가.
     * 아웃바운드는 상담사가 먼저 전화한 상황이므로 응답률을 40%로 낮게 설정한다.
     */
    private void insertClientReviews(List<Long> consultIds, ThreadLocalRandom random) {
        List<Object[]> args = new ArrayList<>();
        for (Long consultId : consultIds) {
            if (random.nextInt(100) >= 40) continue; // 60% 미응답

            int s1 = 1 + random.nextInt(5);
            int s2 = 1 + random.nextInt(5);
            int s3 = 1 + random.nextInt(5);
            int s4 = 1 + random.nextInt(5);
            int s5 = 1 + random.nextInt(5);
            double avg = Math.round((s1 + s2 + s3 + s4 + s5) / 5.0 * 10) / 10.0;
            args.add(new Object[]{consultId, s1, s2, s3, s4, s5, avg});
        }
        if (!args.isEmpty()) {
            jdbcTemplate.batchUpdate(
                    "INSERT INTO client_review (consult_id, score_1, score_2, score_3, score_4, score_5, score_average) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    args
            );
        }
    }

    /**
     * customer_risk_logs — 위험 고객 감지 로그.
     * 아웃바운드 대상은 이탈 위험 고객이므로 CHURN 유형으로 항상 생성한다.
     * CHURN 70% / COMP 30% 비율로 분배한다.
     */
    private void insertCustomerRiskLogs(List<Long> consultIds, List<Integer> empIds,
                                        List<Long> customerIds, ThreadLocalRandom random) {
        List<Object[]> args = new ArrayList<>();
        String[] riskLevels = {"LOW", "LOW", "LOW", "MEDIUM", "MEDIUM", "HIGH", "CRITICAL"};
        for (int i = 0; i < consultIds.size(); i++) {
            // 아웃바운드 대상은 이탈 위험 고객 → 항상 risk log 생성
            String typeCode  = random.nextInt(100) < 70 ? "CHURN" : "COMP";
            String levelCode = riskLevels[random.nextInt(riskLevels.length)];
            args.add(new Object[]{consultIds.get(i), empIds.get(i), customerIds.get(i), typeCode, levelCode});
        }
        if (!args.isEmpty()) {
            jdbcTemplate.batchUpdate(
                    "INSERT INTO customer_risk_logs (consult_id, emp_id, customer_id, type_code, level_code) " +
                    "VALUES (?, ?, ?, ?, ?)",
                    args
            );
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  유틸리티
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * iam_memo — 아웃바운드 통화 결과를 기록한다.
     * <ul>
     *   <li>CONVERTED: {@code CONVERTED - <defenseCategory>}</li>
     *   <li>REJECTED:  {@code REJECTED - <outboundCategory>}</li>
     * </ul>
     */
    private String buildMemo(OutboundTextResult textResult) {
        if ("CONVERTED".equals(textResult.callResult())) {
            return "CONVERTED - " + textResult.defenseCategory();
        }
        return "REJECTED - " + textResult.outboundCategory();
    }

    /**
     * outbound_category code_name을 분포에 따라 선택한다.
     *
     * <p>분포 설계:
     * <ul>
     *   <li>CONVERTED (전환 성공): 45%</li>
     *   <li>COST (요금/비용 문제): 16% — 가장 흔한 거절 사유</li>
     *   <li>SWITCH (타사 전환): 11%</li>
     *   <li>DISSATISFIED (서비스 불만): 10%</li>
     *   <li>NO_NEED (필요 없음): 8%</li>
     *   <li>CONSIDER (고려 중): 6%</li>
     *   <li>OTHER (기타): 4%</li>
     * </ul>
     *
     * @param categoryType "CHN" | "FEE" | "TRB"
     * @param roll         0~99 난수
     * @return "CONVERTED" 또는 analysis_code.outbound_category code_name
     */
    private String pickOutboundCategoryKey(String categoryType, int roll) {
        return switch (categoryType) {
            case "FEE" -> pickFeeOutboundCategoryKey(roll);
            case "TRB" -> pickTrbOutboundCategoryKey(roll);
            default    -> pickChnOutboundCategoryKey(roll);
        };
    }

    /** CHN: CONVERTED 45% / COST 16% / SWITCH 11% / DISSATISFIED 10% / NO_NEED 8% / CONSIDER 6% / OTHER 4% */
    private String pickChnOutboundCategoryKey(int roll) {
        if (roll < 45) return "CONVERTED";
        if (roll < 61) return "COST";
        if (roll < 72) return "SWITCH";
        if (roll < 82) return "DISSATISFIED";
        if (roll < 90) return "NO_NEED";
        if (roll < 96) return "CONSIDER";
        return "OTHER";
    }

    /** FEE: CONVERTED 40% / COST 35% / CONSIDER 12% / SWITCH 8% / OTHER 5% */
    private String pickFeeOutboundCategoryKey(int roll) {
        if (roll < 40) return "CONVERTED";
        if (roll < 75) return "COST";
        if (roll < 87) return "CONSIDER";
        if (roll < 95) return "SWITCH";
        return "OTHER";
    }

    /** TRB: CONVERTED 45% / DISSATISFIED 30% / CONSIDER 12% / SWITCH 8% / OTHER 5% */
    private String pickTrbOutboundCategoryKey(int roll) {
        if (roll < 45) return "CONVERTED";
        if (roll < 75) return "DISSATISFIED";
        if (roll < 87) return "CONSIDER";
        if (roll < 95) return "SWITCH";
        return "OTHER";
    }

    /** 카테고리 유형 선택: CHN 60% / FEE 25% / TRB 15% */
    private String pickCategoryType(int roll) {
        if (roll < 60) return "CHN";
        if (roll < 85) return "FEE";
        return "TRB";
    }

    private LocalDateTime randomBusinessTime(LocalDate targetDate, ThreadLocalRandom random) {
        int hour   = 8 + random.nextInt(10); // 08 ~ 17시
        int minute = random.nextInt(60);
        int second = random.nextInt(60);
        return targetDate.atTime(hour, minute, second);
    }
}
