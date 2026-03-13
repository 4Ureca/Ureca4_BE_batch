package com.uplus.batch.synthetic;

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
 *   <li>retention_analysis: 아웃바운드 더미 데이터 즉시 삽입
 *       (outbound_call_result·outbound_category·defense_category 포함 — V34 기준)</li>
 *   <li>AI 추출 트리거 없음: result_event_status·summary_event_status 미생성</li>
 * </ul>
 *
 * <p>§2 분포 조건:
 * <ul>
 *   <li>결과: 템플릿 비율에 따라 약 45% CONVERTED / 55% REJECTED</li>
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


    public record BatchResult(List<Long> consultIds) {
        public boolean isEmpty() { return consultIds.isEmpty(); }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Step 1: 아웃바운드 consultation_results + raw_texts + retention_analysis
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

        if (agents.isEmpty() || customers.isEmpty() || chnCodes.isEmpty()) {
            log.warn("[OutboundFactory] 상담사·고객·CHN 카테고리 없음 — Step1 스킵");
            return new BatchResult(List.of());
        }

        String resultSql = """
                INSERT INTO consultation_results
                    (emp_id, customer_id, channel, category_code, duration_sec,
                     iam_issue, iam_action, iam_memo, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        List<Long>         consultIds  = new ArrayList<>(batchSize);
        List<Integer>      empIds      = new ArrayList<>(batchSize);
        List<Long>         customerIds = new ArrayList<>(batchSize);
        List<OutboundTextResult> textResults = new ArrayList<>(batchSize);

        for (int i = 0; i < batchSize; i++) {
            var agent    = agents.get(random.nextInt(agents.size()));
            var customer = customers.get(random.nextInt(customers.size()));
            String categoryCode = chnCodes.get(random.nextInt(chnCodes.size()));
            int durationSec = 120 + random.nextInt(481); // CALL: 120~600s

            OutboundTextResult textResult = outboundRawTextGenerator.generate(
                    new Random(ThreadLocalRandom.current().nextLong()));
            textResults.add(textResult);

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

            consultIds.add(keyHolder.getKey().longValue());
            empIds.add(empId);
            customerIds.add(custId);
        }

        // ── consultation_raw_texts Bulk INSERT ──────────────────────────────
        List<Object[]> rawArgs = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            rawArgs.add(new Object[]{consultIds.get(i), textResults.get(i).rawTextJson()});
        }
        jdbcTemplate.batchUpdate(
                "INSERT INTO consultation_raw_texts (consult_id, raw_text_json) VALUES (?, ?)",
                rawArgs
        );

        // ── retention_analysis Bulk INSERT (아웃바운드 더미 — V34 컬럼 포함) ──
        insertRetentionAnalysis(consultIds, textResults);

        // ── 연관 테이블 Bulk INSERT ──────────────────────────────────────────
        insertClientReviews(consultIds, random);
        insertCustomerRiskLogs(consultIds, empIds, customerIds, random);

        log.info("[OutboundFactory] Step1 완료 — {}건 생성 | consultId 범위: {} ~ {}",
                batchSize, consultIds.get(0), consultIds.get(consultIds.size() - 1));

        return new BatchResult(consultIds);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  retention_analysis 더미 데이터 삽입 (V34 컬럼 포함)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 아웃바운드 상담 결과 기반으로 retention_analysis 더미 데이터를 삽입한다.
     *
     * <p>complaintCategory·defenseCategory는 {@link OutboundRawTextGenerator} 템플릿에서
     * 결정된 값을 그대로 사용한다 (랜덤 배정 없음).
     *
     * <ul>
     *   <li>CONVERTED: defense_success=true, defenseCategory=템플릿 방어코드, outbound_category=null</li>
     *   <li>REJECTED:  defense_success=false, defenseCategory=ADM_CLOSE_FAIL/ADM_GUIDE, outbound_category=거절사유코드</li>
     * </ul>
     */
    private void insertRetentionAnalysis(List<Long> consultIds,
                                         List<OutboundTextResult> textResults) {
        List<Object[]> args = new ArrayList<>(consultIds.size());
        for (int i = 0; i < consultIds.size(); i++) {
            OutboundTextResult tr = textResults.get(i);
            boolean isConverted = "CONVERTED".equals(tr.callResult());

            String defenseCategory   = tr.defenseCategory();
            String complaintCategory = tr.complaintCategory();

            String defenseActionsJson = "[\"" + defenseCategory + "\"]";

            String rawSummary = isConverted
                    ? "아웃바운드 상담 - 전환 성공"
                    : "아웃바운드 상담 - 고객 거절 (" + tr.outboundCategory() + ")";

            args.add(new Object[]{
                    consultIds.get(i),     // consult_id
                    true,                  // has_intent (아웃바운드 대상 = 이탈 위험 고객)
                    null,                  // complaint_reason (AI 분석에서 채울 예정)
                    true,                  // defense_attempted (항상 방어 시도)
                    isConverted,           // defense_success
                    defenseActionsJson,    // defense_actions
                    rawSummary,            // raw_summary
                    tr.callResult(),       // outbound_call_result (CONVERTED | REJECTED)
                    null,                  // outbound_report (AI 분석 미완료)
                    complaintCategory,     // complaint_category (FK → analysis_code)
                    defenseCategory,       // defense_category   (FK → analysis_code)
                    tr.outboundCategory()  // outbound_category  (FK → analysis_code, REJECTED 시 사유 코드)
            });
        }
        jdbcTemplate.batchUpdate(
                """
                INSERT INTO retention_analysis
                    (consult_id, has_intent, complaint_reason, defense_attempted, defense_success,
                     defense_actions, raw_summary, outbound_call_result, outbound_report,
                     complaint_category, defense_category, outbound_category,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
                """,
                args
        );
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
     * iam_memo 태그를 생성한다.
     * <ul>
     *   <li>CONVERTED: {@code [SYNTHETIC][OUTBOUND] CONVERTED - <defenseCategory>}</li>
     *   <li>REJECTED:  {@code [SYNTHETIC][OUTBOUND] REJECTED - <outboundCategory>}</li>
     * </ul>
     */
    private String buildMemo(OutboundTextResult textResult) {
        if ("CONVERTED".equals(textResult.callResult())) {
            return "[SYNTHETIC][OUTBOUND] CONVERTED - " + textResult.defenseCategory();
        }
        return "[SYNTHETIC][OUTBOUND] REJECTED - " + textResult.outboundCategory();
    }

    private LocalDateTime randomBusinessTime(LocalDate targetDate, ThreadLocalRandom random) {
        int hour   = 8 + random.nextInt(10); // 08 ~ 17시
        int minute = random.nextInt(60);
        int second = random.nextInt(60);
        return targetDate.atTime(hour, minute, second);
    }
}
