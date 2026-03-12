package com.uplus.batch.synthetic;

import com.uplus.batch.common.dummy.dto.CacheDummy;
import com.uplus.batch.domain.summary.entity.SummaryEventStatus;
import com.uplus.batch.domain.summary.repository.SummaryEventStatusRepository;
import com.uplus.batch.jobs.raw_text_dummy.generator.RawTextGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 합성 상담 데이터 배치 생성 오케스트레이터.
 *
 * <p>호출 방식 (Spring @Transactional 우회 방지):
 * 각 Step 메서드를 스케줄러에서 순차적으로 외부 호출한다.
 * — self-invocation(this.method())으로 호출하면 @Transactional이 무시되므로
 *   {@link SyntheticDataGeneratorScheduler}가 아래 순서로 직접 호출:
 * <ol>
 *   <li>{@link #executeStep1(int)} — @Transactional, 반환 시 커밋 완료</li>
 *   <li>{@link #triggerAiExtraction(List, List)} — Step1 커밋 후 별도 실행</li>
 *   <li>{@link #triggerSummaryGeneration(List)} — Step1 커밋 후 별도 실행</li>
 * </ol>
 *
 * <p>§2 분포 조건:
 * <ul>
 *   <li>카테고리: CHN 40% / TRB 20% / FEE 20% / 기타(ADD·ETC) 20%</li>
 *   <li>채널: CALL 70% / CHATTING 30%</li>
 *   <li>durationSec: CALL 120~600s / CHATTING 60~300s</li>
 *   <li>합성 데이터 식별: iam_memo에 [SYNTHETIC] 태그 삽입</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SyntheticConsultationFactory {

    private final SyntheticPersonMatcher personMatcher;
    private final RawTextGenerator rawTextGenerator;
    private final JdbcTemplate jdbcTemplate;
    private final SummaryEventStatusRepository summaryEventStatusRepo;
    private final CacheDummy cacheDummy;

    // ── 위험 유형·등급 (risk_type_policy / risk_level_policy 참조) ─────────
    private static final List<String> RISK_TYPES  = List.of("CHURN","FRAUD","PHISHING","ABUSE","COMP");
    // LOW 가중치 높게 (실제 분포 반영)
    private static final List<String> RISK_LEVELS = List.of("LOW","LOW","LOW","MEDIUM","MEDIUM","HIGH","CRITICAL");

    // ─── IAM 템플릿 (카테고리 접두사 → [iam_issue, iam_action, iam_memo]) ─────────────
    private static final Map<String, List<String[]>> IAM_TEMPLATES = Map.of(
            "FEE", List.of(
                    new String[]{"5G 요금제 변경 시 혜택 및 비용 문의", "최적 5G 요금제 안내 후 변경 처리 완료", "다음 달부터 신규 요금 적용 예정"},
                    new String[]{"청구서 항목 이중 청구 이상 문의", "이중 청구 오류 확인 및 정정 처리", "익월 청구서에서 차감 예정, 고객 동의 완료"},
                    new String[]{"결합 할인 미적용 건 확인 요청", "결합 할인 재적용 요청 시스템 등록 완료", "1~3 영업일 내 처리 후 SMS 발송 예정"}
            ),
            "DEV", List.of(
                    new String[]{"기기변경 공시지원금 적용 조건 문의", "기기 목록 및 지원금 조건 안내 후 예약 접수", "기기변경 예약 접수 완료, 방문 일정 SMS 발송"},
                    new String[]{"단말기 할부금 잔여액 및 소멸 일정 확인 요청", "현행 할부 계획 조회 후 잔여 할부금 상세 안내", "안내 완료, 추가 문의 없음"},
                    new String[]{"유심 분실로 인한 재발급 요청", "본인 인증 후 유심 재발급 접수 처리", "3~5 영업일 이내 등기 배송 예정"}
            ),
            "TRB", List.of(
                    new String[]{"인터넷 속도 저하 불만 접수", "원격 점검 후 현장 기사 출동 요청 등록", "출동 일정 고객 협의 완료, 익일 방문 예정"},
                    new String[]{"TV 화면 재생 오류 및 셋톱박스 이상 문의", "셋톱박스 재부팅 가이드 및 원격 점검 실시", "원격 조치 완료, 정상 작동 확인"},
                    new String[]{"모바일 데이터 연결 불가 문의", "APN 설정 초기화 및 네트워크 재설정 가이드 안내", "고객 직접 조치 후 정상 연결 확인"}
            ),
            "CHN", List.of(
                    new String[]{"약정 만료 후 해지 의사 표명", "잔여 혜택 및 재약정 조건 비교 안내, 재약정 유도", "고객 재약정 결정, 신규 약정 등록 완료"},
                    new String[]{"중도 해지 시 위약금 금액 조회 요청", "현행 약정 기준 위약금 계산 결과 상세 안내", "위약금 확인 완료, 해지 여부 고객 재검토 예정"},
                    new String[]{"타사 이동을 위한 번호이동 해지 요청", "번호이동 절차 안내 및 잔류 혜택 비교 제안 진행", "이탈 방어 미성공, 번호이동 해지 처리 완료"}
            ),
            "ADD", List.of(
                    new String[]{"해외 출국 전 로밍 서비스 가입 문의", "이용 국가별 로밍 요금제 비교 안내 및 최적 상품 추천", "출국 3일 전 자동 개통 예약 완료"},
                    new String[]{"구독 중인 OTT 서비스 변경 요청", "현행 부가서비스 현황 조회 후 요청 서비스로 변경 처리", "다음 달 1일부터 변경 사항 적용 예정"}
            ),
            "ETC", List.of(
                    new String[]{"인근 U+ 공식 매장 위치 문의", "고객 주소 기반 인근 매장 3곳 안내", "안내 완료"},
                    new String[]{"서비스 이용 불만 접수 및 처리 요청", "불만 내용 청취 후 담당 부서 이관 처리", "처리 결과 3 영업일 이내 회신 예정"}
            )
    );

    // ─────────────────────────────────────────────────────────
    //  Step 1: consultation_results + raw_texts INSERT (단일 트랜잭션)
    // ─────────────────────────────────────────────────────────

    /**
     * consultation_results와 consultation_raw_texts를 하나의 트랜잭션으로 삽입한다.
     * 이 메서드는 반드시 외부 Bean(SyntheticDataGeneratorScheduler)에서 호출해야
     * Spring @Transactional이 정상 동작한다.
     *
     * @return 생성된 (consultId 목록, categoryCode 목록) 쌍
     */
    @Transactional
    public BatchResult executeStep1(int batchSize) {
        var random    = ThreadLocalRandom.current();
        var agents    = personMatcher.getAgents();
        var customers = personMatcher.getCustomers();

        if (agents.isEmpty() || customers.isEmpty()) {
            log.warn("[SyntheticFactory] 상담사 또는 고객 데이터 없음 — Step1 스킵");
            return new BatchResult(List.of(), List.of());
        }

        String resultSql = """
                INSERT INTO consultation_results
                    (emp_id, customer_id, channel, category_code, duration_sec,
                     iam_issue, iam_action, iam_memo, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        List<Long>    consultIds    = new ArrayList<>(batchSize);
        List<String>  categoryCodes = new ArrayList<>(batchSize);
        List<String>  channels      = new ArrayList<>(batchSize);
        List<Integer> empIds        = new ArrayList<>(batchSize);
        List<Long>    customerIds   = new ArrayList<>(batchSize);

        for (int i = 0; i < batchSize; i++) {
            var agent    = agents.get(random.nextInt(agents.size()));
            var customer = customers.get(random.nextInt(customers.size()));

            String categoryCode = pickCategoryCode(random.nextInt(100));
            String channel      = random.nextInt(100) < 70 ? "CALL" : "CHATTING";
            int    durationSec  = "CALL".equals(channel)
                    ? 120 + random.nextInt(481)   // CALL: 120~600s
                    : 60  + random.nextInt(241);  // CHATTING: 60~300s

            String[] iam  = pickIamTemplate(categoryCode, random.nextInt(10));
            String   memo = "[SYNTHETIC] " + iam[2];

            // ── PreparedStatementCreator는 effectively-final 변수만 캡처 가능 ──
            final int    empId      = agent.empId();
            final long   customerId = customer.customerId();
            final String ch         = channel;
            final String catCode    = categoryCode;
            final int    dur        = durationSec;
            final String issue      = iam[0];
            final String action     = iam[1];
            final String finalMemo  = memo;
            final LocalDateTime now = LocalDateTime.now();

            KeyHolder keyHolder = new GeneratedKeyHolder();
            jdbcTemplate.update(con -> {
                PreparedStatement ps = con.prepareStatement(resultSql, Statement.RETURN_GENERATED_KEYS);
                ps.setInt(1, empId);
                ps.setLong(2, customerId);
                ps.setString(3, ch);
                ps.setString(4, catCode);
                ps.setInt(5, dur);
                ps.setString(6, issue);
                ps.setString(7, action);
                ps.setString(8, finalMemo);
                ps.setObject(9, now);
                return ps;
            }, keyHolder);

            consultIds.add(keyHolder.getKey().longValue());
            categoryCodes.add(categoryCode);
            channels.add(channel);
            empIds.add(empId);
            customerIds.add(customerId);
        }

        // ── consultation_raw_texts Bulk INSERT ──
        List<Object[]> rawArgs = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            rawArgs.add(new Object[]{
                    consultIds.get(i),
                    rawTextGenerator.generate(categoryCodes.get(i), channels.get(i),
                            new java.util.Random(ThreadLocalRandom.current().nextLong()))
            });
        }
        jdbcTemplate.batchUpdate(
                "INSERT INTO consultation_raw_texts (consult_id, raw_text_json) VALUES (?, ?)",
                rawArgs
        );

        // ── 연관 테이블 Bulk INSERT (같은 트랜잭션) ───────────────────────
        insertClientReviews(consultIds, random);
        insertCustomerRiskLogs(consultIds, empIds, customerIds, categoryCodes, random);
        insertProductLogs(consultIds, customerIds, categoryCodes, random);

        log.info("[SyntheticFactory] Step1 완료 — consultation_results: {}건, raw_texts: {}건 | " +
                        "consultId 범위: {} ~ {}",
                batchSize, batchSize,
                consultIds.get(0), consultIds.get(consultIds.size() - 1));

        return new BatchResult(consultIds, categoryCodes);
    }

    // ─────────────────────────────────────────────────────────
    //  Step 2: result_event_status REQUESTED (Step 1 커밋 후)
    // ─────────────────────────────────────────────────────────

    public void triggerAiExtraction(List<Long> consultIds, List<String> categoryCodes) {
        List<Object[]> args = new ArrayList<>(consultIds.size());
        for (int i = 0; i < consultIds.size(); i++) {
            args.add(new Object[]{consultIds.get(i), categoryCodes.get(i)});
        }

        jdbcTemplate.batchUpdate(
                """
                INSERT INTO result_event_status
                    (consult_id, category_code, status, retry_count, created_at, updated_at)
                VALUES (?, ?, 'REQUESTED', 0, NOW(), NOW())
                """,
                args
        );
    }

    // ─────────────────────────────────────────────────────────
    //  Step 3: summary_event_status REQUESTED (Step 1 커밋 후)
    // ─────────────────────────────────────────────────────────

    public void triggerSummaryGeneration(List<Long> consultIds) {
        List<SummaryEventStatus> events = new ArrayList<>(consultIds.size());
        for (Long cid : consultIds) {
            events.add(SummaryEventStatus.builder().consultId(cid).build());
        }
        summaryEventStatusRepo.saveAll(events);
    }

    // ─────────────────────────────────────────────────────────
    //  내부 DTO
    // ─────────────────────────────────────────────────────────

    // ─────────────────────────────────────────────────────────
    //  연관 테이블 INSERT 헬퍼
    // ─────────────────────────────────────────────────────────

    /**
     * client_review — 고객 만족도 평가 (70% 확률로 생성).
     * score_1~5: 1~5점 랜덤, score_average: 평균.
     */
    private void insertClientReviews(List<Long> consultIds, ThreadLocalRandom random) {
        List<Object[]> args = new ArrayList<>();
        for (Long consultId : consultIds) {
            if (random.nextInt(100) >= 70) continue; // 30% 미응답

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
     * customer_risk_logs — 위험 고객 감지 로그 (40% 확률로 생성).
     * CHN 카테고리는 CHURN 유형 우선, 나머지는 랜덤.
     */
    private void insertCustomerRiskLogs(List<Long> consultIds, List<Integer> empIds,
                                        List<Long> customerIds, List<String> categoryCodes,
                                        ThreadLocalRandom random) {
        List<Object[]> args = new ArrayList<>();
        for (int i = 0; i < consultIds.size(); i++) {
            if (random.nextInt(100) >= 40) continue; // 60% 해당 없음

            String typeCode;
            if (categoryCodes.get(i).contains("CHN")) {
                // CHN은 CHURN 60%, 나머지 40%
                typeCode = random.nextInt(100) < 60 ? "CHURN"
                        : RISK_TYPES.get(random.nextInt(RISK_TYPES.size()));
            } else {
                typeCode = RISK_TYPES.get(random.nextInt(RISK_TYPES.size()));
            }
            String levelCode = RISK_LEVELS.get(random.nextInt(RISK_LEVELS.size()));

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

    /**
     * consult_product_logs — 상담 중 처리된 상품 변경 이력.
     * 60% 확률로 1건, 20% 확률로 2건, 20% 0건.
     * contract_type에 따라 new/canceled 컬럼을 구분하여 삽입.
     */
    private void insertProductLogs(List<Long> consultIds, List<Long> customerIds,
                                   List<String> categoryCodes, ThreadLocalRandom random) {
        boolean hasCodes = cacheDummy.getHomeProductCodes() != null && !cacheDummy.getHomeProductCodes().isEmpty()
                && cacheDummy.getMobileProductCodes() != null && !cacheDummy.getMobileProductCodes().isEmpty()
                && cacheDummy.getAdditionalProductCodes() != null && !cacheDummy.getAdditionalProductCodes().isEmpty();
        if (!hasCodes) return;

        List<Object[]> args = new ArrayList<>();
        String[] contractTypes = {"NEW", "CANCEL", "CHANGE", "RENEW"};
        String[] productTypes  = {"home", "mobile", "additional"};

        for (int i = 0; i < consultIds.size(); i++) {
            int logCount;
            int roll = random.nextInt(100);
            if (roll < 20) continue;          // 20% 상품 변경 없음
            else if (roll < 80) logCount = 1; // 60% 1건
            else logCount = 2;                // 20% 2건

            for (int j = 0; j < logCount; j++) {
                // CHN은 RENEW/CANCEL 위주, 나머지는 랜덤
                String contractType;
                if (categoryCodes.get(i).contains("CHN")) {
                    contractType = random.nextBoolean() ? "RENEW" : "CANCEL";
                } else {
                    contractType = contractTypes[random.nextInt(contractTypes.length)];
                }
                String productType = productTypes[random.nextInt(productTypes.length)];
                String code1 = pickProductCode(productType, random);
                String code2 = "CHANGE".equals(contractType) ? pickDifferentCode(productType, code1, random) : null;

                args.add(buildProductLogArgs(
                        consultIds.get(i), customerIds.get(i),
                        contractType, productType, code1, code2));
            }
        }
        if (!args.isEmpty()) {
            jdbcTemplate.batchUpdate(
                    "INSERT INTO consult_product_logs " +
                    "(consult_id, customer_id, contract_type, product_type, " +
                    "new_product_home, new_product_mobile, new_product_service, " +
                    "canceled_product_home, canceled_product_mobile, canceled_product_service) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    args
            );
        }
    }

    /** product_type × contract_type 조합으로 10개 컬럼 Object[] 생성 */
    private Object[] buildProductLogArgs(long consultId, long customerId,
                                         String contractType, String productType,
                                         String code1, String code2) {
        // [new_home, new_mobile, new_service, cancel_home, cancel_mobile, cancel_service]
        String[] cols = new String[6];
        int newIdx    = switch (productType) { case "home" -> 0; case "mobile" -> 1; default -> 2; };
        int cancelIdx = newIdx + 3;

        boolean isNew    = "NEW".equals(contractType) || "RENEW".equals(contractType);
        boolean isCancel = "CANCEL".equals(contractType);
        boolean isChange = "CHANGE".equals(contractType);

        if (isNew)    cols[newIdx]    = code1;
        if (isCancel) cols[cancelIdx] = code1;
        if (isChange) { cols[newIdx] = code1; cols[cancelIdx] = code2; }

        return new Object[]{consultId, customerId, contractType, productType,
                cols[0], cols[1], cols[2], cols[3], cols[4], cols[5]};
    }

    private String pickProductCode(String productType, ThreadLocalRandom random) {
        List<String> pool = switch (productType) {
            case "home"       -> cacheDummy.getHomeProductCodes();
            case "mobile"     -> cacheDummy.getMobileProductCodes();
            default           -> cacheDummy.getAdditionalProductCodes();
        };
        return pool.get(random.nextInt(pool.size()));
    }

    private String pickDifferentCode(String productType, String exclude, ThreadLocalRandom random) {
        List<String> pool = switch (productType) {
            case "home"   -> cacheDummy.getHomeProductCodes();
            case "mobile" -> cacheDummy.getMobileProductCodes();
            default       -> cacheDummy.getAdditionalProductCodes();
        };
        if (pool.size() == 1) return pool.get(0); // 단일 코드면 그대로
        String code;
        do { code = pool.get(random.nextInt(pool.size())); } while (code.equals(exclude));
        return code;
    }

    public record BatchResult(List<Long> consultIds, List<String> categoryCodes) {
        public boolean isEmpty() { return consultIds.isEmpty(); }
    }

    // ─────────────────────────────────────────────────────────
    //  카테고리 분포 선택 (§2-1)
    // ─────────────────────────────────────────────────────────

    /** CHN 40% / TRB 20% / FEE 20% / 기타(ADD·ETC) 20% */
    private String pickCategoryCode(int roll) {
        List<String> pool;
        if      (roll < 40) pool = personMatcher.getChnCodes();
        else if (roll < 60) pool = personMatcher.getTrbCodes();
        else if (roll < 80) pool = personMatcher.getFeeCodes();
        else                pool = personMatcher.getOtherCodes();
        return pool.get(ThreadLocalRandom.current().nextInt(pool.size()));
    }

    // ─────────────────────────────────────────────────────────
    //  IAM 템플릿 선택
    // ─────────────────────────────────────────────────────────

    /** categoryCode 접두사(M_FEE_01 → "FEE")로 IAM 세트 선택. 미매핑 시 ETC 폴백. */
    private String[] pickIamTemplate(String categoryCode, int randomIndex) {
        String key = "ETC";
        if (categoryCode != null) {
            String[] parts = categoryCode.split("_");
            if (parts.length >= 2) key = parts[1];
        }
        List<String[]> templates = IAM_TEMPLATES.getOrDefault(key, IAM_TEMPLATES.get("ETC"));
        return templates.get(randomIndex % templates.size());
    }
}
