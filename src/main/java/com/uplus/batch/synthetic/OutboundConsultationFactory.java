package com.uplus.batch.synthetic;

import com.uplus.batch.common.dummy.dto.CacheDummy;
import com.uplus.batch.domain.summary.entity.SummaryEventStatus;
import com.uplus.batch.domain.summary.repository.ProductRepository;
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
 *   <li>결과: CHN CONVERTED 10% / UPSELL CONVERTED 60% / ARREARS CONVERTED 75% / TRB CONVERTED 35%</li>
 *   <li>고객 만족도 평가: 100% (모든 상담에 대해 항상 생성)</li>
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
    private final CacheDummy cacheDummy;
    private final ProductRepository productRepository;

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
        var agents         = personMatcher.getAgents();
        var customers      = personMatcher.getCustomers();
        var otbChnCodes    = personMatcher.getOtbChnCodes();
        var otbUpsellCodes = personMatcher.getOtbUpsellCodes();
        var otbArrearsCodes= personMatcher.getOtbArrearsCodes();
        var otbTrbCodes     = personMatcher.getOtbTrbCodes();
        var otbNewSubsCodes = personMatcher.getOtbNewSubsCodes();

        if (agents.isEmpty() || customers.isEmpty() || otbChnCodes.isEmpty()) {
            log.warn("[OutboundFactory] 상담사·고객·OTB 카테고리 없음 — Step1 스킵");
            return new BatchResult(List.of(), List.of());
        }

        String resultSql = """
                INSERT INTO consultation_results
                    (emp_id, customer_id, channel, category_code, duration_sec,
                     iam_issue, iam_action, iam_memo, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        List<Long>    consultIds          = new ArrayList<>(batchSize);
        List<String>  categoryCodes       = new ArrayList<>(batchSize);
        List<Integer> empIds              = new ArrayList<>(batchSize);
        List<Long>    customerIds         = new ArrayList<>(batchSize);
        List<String>  categoryTypes       = new ArrayList<>(batchSize);
        List<String>  callResults          = new ArrayList<>(batchSize);
        List<String>  outboundCategories  = new ArrayList<>(batchSize);
        List<Boolean> cancelProcesseds    = new ArrayList<>(batchSize);
        List<String>  targetMobileCodes   = new ArrayList<>(batchSize);
        List<String>  customerMobileCodes = new ArrayList<>(batchSize);

        List<Object[]> rawArgs = new ArrayList<>(batchSize);

        for (int i = 0; i < batchSize; i++) {
            var agent    = agents.get(random.nextInt(agents.size()));
            var customer = customers.get(random.nextInt(customers.size()));
            String categoryType = pickCategoryType(random.nextInt(100)); // CHN 30% / NEW_SUBS 30% / UPSELL 20% / ARREARS 10% / TRB 10%
            List<String> otbPool = switch (categoryType) {
                case "UPSELL"   -> otbUpsellCodes;
                case "ARREARS"  -> otbArrearsCodes;
                case "TRB"      -> otbTrbCodes;
                case "NEW_SUBS" -> otbNewSubsCodes;
                default         -> otbChnCodes;
            };
            String categoryCode = otbPool.get(random.nextInt(otbPool.size())); // M_OTB_* (타입 연동)
            int durationSec = 120 + random.nextInt(481); // CALL: 120~600s

            String outboundCategoryKey = pickOutboundCategoryKey(categoryType, random.nextInt(100));

            // UPSELL / NEW_SUBS CONVERTED 시 변경 대상 요금제 코드·명칭 사전 선택
            boolean willConvert = "CONVERTED".equals(outboundCategoryKey);
            boolean hasMobileCodes = cacheDummy.getMobileProductCodes() != null
                    && !cacheDummy.getMobileProductCodes().isEmpty();
            boolean isUpsellConverted  = willConvert && "UPSELL".equals(categoryType)   && hasMobileCodes;
            boolean isNewSubsConverted = willConvert && "NEW_SUBS".equals(categoryType) && hasMobileCodes;
            String targetMobileCode = (isUpsellConverted || isNewSubsConverted)
                    ? pickDifferentCode("mobile",
                            customer.mobileCode() != null ? customer.mobileCode() : "", random)
                    : null;
            String targetMobilePlanName = targetMobileCode != null
                    ? productRepository.findProductName(targetMobileCode)
                    : null;

            OutboundRawTextGenerator.CustomerContext ctx = new OutboundRawTextGenerator.CustomerContext(
                    customer.name(),
                    customer.mobilePlanName(),
                    customer.homePlanName(),
                    targetMobilePlanName
            );
            OutboundTextResult textResult = outboundRawTextGenerator.generate(
                    categoryType, outboundCategoryKey,
                    new Random(ThreadLocalRandom.current().nextLong()), ctx);

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
            categoryTypes.add(categoryType);
            callResults.add(textResult.callResult());
            outboundCategories.add(textResult.outboundCategory());
            cancelProcesseds.add(textResult.cancelProcessed());
            targetMobileCodes.add(targetMobileCode);
            customerMobileCodes.add(customer.mobileCode());
            rawArgs.add(new Object[]{consultId, textResult.rawTextJson()});
        }

        // ── consultation_raw_texts Bulk INSERT ──────────────────────────────
        jdbcTemplate.batchUpdate(
                "INSERT INTO consultation_raw_texts (consult_id, raw_text_json) VALUES (?, ?)",
                rawArgs
        );

        // ── 연관 테이블 Bulk INSERT ──────────────────────────────────────────
        insertClientReviews(consultIds, callResults, outboundCategories, random);
        insertCustomerRiskLogs(consultIds, empIds, customerIds, random);
        insertProductLogs(consultIds, customerIds, categoryTypes, callResults,
                cancelProcesseds, targetMobileCodes, customerMobileCodes, random);

        log.info("[OutboundFactory] Step1 완료 — {}건 생성 | consultId 범위: {} ~ {}",
                batchSize, consultIds.get(0), consultIds.get(consultIds.size() - 1));

        return new BatchResult(consultIds, categoryCodes);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Step 2: result_event_status, excellent_event_status REQUESTED (OUTBOUND, Step1 커밋 후)
    // ─────────────────────────────────────────────────────────────────────────

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
    public void triggerExcellentScoring(List<Long> consultIds) {
        List<Object[]> args = new ArrayList<>(consultIds.size());
        for (Long consultId : consultIds) {
            args.add(new Object[]{consultId});
        }
        
        jdbcTemplate.batchUpdate(
                """
                INSERT INTO excellent_event_status
                    (consult_id, status, retry_count, created_at, updated_at)
                VALUES (?, 'REQUESTED', 0, NOW(), NOW())
                """,
                args
        );
        log.info("[OutboundFactory] {}건 채점 이벤트(excellent) 발행 완료", consultIds.size());
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
     *
     * <p>아웃바운드 통화 결과(= 상담 원문의 결말)로 기반 만족도를 산출한다.
     *
     * <ul>
     *   <li>CONVERTED → base 4 (성공적 결과, 고객 긍정)</li>
     *   <li>REJECTED + DISSATISFIED → base 1 (서비스 불만)</li>
     *   <li>REJECTED + COST / SWITCH / OTHER → base 2 (부정적 거절)</li>
     *   <li>REJECTED + NO_NEED / CONSIDER → base 3 (중립적 거절)</li>
     * </ul>
     */
    private void insertClientReviews(List<Long> consultIds, List<String> callResults,
                                     List<String> outboundCategories, ThreadLocalRandom random) {
        List<Object[]> args = new ArrayList<>();
        for (int i = 0; i < consultIds.size(); i++) {
            int base = baseSatisfactionFromOutbound(callResults.get(i), outboundCategories.get(i));
            int s1 = satisfactionScore(base, random);
            int s2 = satisfactionScore(base, random);
            int s3 = satisfactionScore(base, random);
            int s4 = satisfactionScore(base, random);
            int s5 = satisfactionScore(base, random);
            double avg = Math.round((s1 + s2 + s3 + s4 + s5) / 5.0 * 10) / 10.0;
            args.add(new Object[]{consultIds.get(i), s1, s2, s3, s4, s5, avg});
        }
        if (!args.isEmpty()) {
            jdbcTemplate.batchUpdate(
                    "INSERT INTO client_review (consult_id, score_1, score_2, score_3, score_4, score_5, score_average) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    args
            );
        }
    }

    /** 아웃바운드 통화 결과로 기반 만족도를 반환한다 (1~5). */
    private int baseSatisfactionFromOutbound(String callResult, String outboundCategory) {
        if ("CONVERTED".equals(callResult)) return 4;
        if (outboundCategory == null) return 2;
        return switch (outboundCategory) {
            case "DISSATISFIED"          -> 1;
            case "COST", "SWITCH"        -> 2;
            case "NO_NEED", "CONSIDER"   -> 3;
            default                      -> 2; // OTHER
        };
    }

    /** 기반 만족도 중심으로 ±2 범위에서 1~5 점수를 생성한다. */
    private int satisfactionScore(int base, ThreadLocalRandom random) {
        return Math.max(1, Math.min(5, base + random.nextInt(5) - 2));
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

    /**
     * consult_product_logs — 아웃바운드 상담에서 실제 처리된 상품 변경 이력.
     *
     * <ul>
     *   <li>CONVERTED + CHN → RENEW (재약정 성공): 고객 현재 모바일 코드 사용</li>
     *   <li>CONVERTED + UPSELL → CHANGE (상위 요금제 전환): 신규=targetMobileCode, 기존=customerMobileCode</li>
     *   <li>CONVERTED + TRB → 로그 없음 (기술 점검·보상, 상품 변경 없음)</li>
     *   <li>CONVERTED + ARREARS → 로그 없음 (납부 완료, 상품 변경 없음)</li>
     *   <li>REJECTED + cancelProcessed → CANCEL: 고객 현재 모바일 코드 사용</li>
     *   <li>REJECTED (일반 거절) → 로그 없음</li>
     * </ul>
     */
    private void insertProductLogs(List<Long> consultIds, List<Long> customerIds,
                                   List<String> categoryTypes, List<String> callResults,
                                   List<Boolean> cancelProcesseds, List<String> targetMobileCodes,
                                   List<String> customerMobileCodes, ThreadLocalRandom random) {
        boolean hasCodes = cacheDummy.getMobileProductCodes() != null
                && !cacheDummy.getMobileProductCodes().isEmpty();
        if (!hasCodes) return;

        List<Object[]> args = new ArrayList<>();
        for (int i = 0; i < consultIds.size(); i++) {
            boolean isConverted     = "CONVERTED".equals(callResults.get(i));
            boolean cancelProcessed = cancelProcesseds.get(i);
            String  categoryType    = categoryTypes.get(i);
            String  custMobileCode  = customerMobileCodes.get(i);

            if (!isConverted) {
                // REJECTED: 상담사가 실제 해지를 처리한 경우만 CANCEL 로그
                if (cancelProcessed && custMobileCode != null) {
                    args.add(buildProductLogArgs(
                            consultIds.get(i), customerIds.get(i),
                            "CANCEL", "mobile", custMobileCode, null));
                }
                continue;
            }

            // TRB·ARREARS CONVERTED: 상품 변경 없음
            if ("TRB".equals(categoryType) || "ARREARS".equals(categoryType)) continue;

            if ("CHN".equals(categoryType)) {
                // 재약정: 고객 기존 상품 코드 유지
                String code = custMobileCode != null ? custMobileCode : pickProductCode("mobile", random);
                args.add(buildProductLogArgs(
                        consultIds.get(i), customerIds.get(i),
                        "RENEW", "mobile", code, null));
            } else if ("UPSELL".equals(categoryType)) {
                // 상위 요금제 전환: 신규 상품(targetMobileCode) → 기존 상품(customerMobileCode)
                String targetCode = targetMobileCodes.get(i);
                String newCode  = targetCode != null ? targetCode : pickProductCode("mobile", random);
                String oldCode  = custMobileCode != null ? custMobileCode
                        : pickDifferentCode("mobile", newCode, random);
                args.add(buildProductLogArgs(
                        consultIds.get(i), customerIds.get(i),
                        "CHANGE", "mobile", newCode, oldCode));
            } else if ("NEW_SUBS".equals(categoryType)) {
                // 신규 가입: 새 상품 코드로 NEW 계약 등록
                String newCode = targetMobileCodes.get(i) != null
                        ? targetMobileCodes.get(i)
                        : pickProductCode("mobile", random);
                args.add(buildProductLogArgs(
                        consultIds.get(i), customerIds.get(i),
                        "NEW", "mobile", newCode, null));
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
        String[] cols = new String[6]; // [new_home, new_mobile, new_service, cancel_home, cancel_mobile, cancel_service]
        int newIdx    = switch (productType) { case "home" -> 0; case "mobile" -> 1; default -> 2; };
        int cancelIdx = newIdx + 3;

        if ("NEW".equals(contractType) || "RENEW".equals(contractType)) cols[newIdx]    = code1;
        else if ("CANCEL".equals(contractType))                          cols[cancelIdx] = code1;
        else if ("CHANGE".equals(contractType)) { cols[newIdx] = code1; cols[cancelIdx] = code2; }

        return new Object[]{consultId, customerId, contractType, productType,
                cols[0], cols[1], cols[2], cols[3], cols[4], cols[5]};
    }

    private String pickProductCode(String productType, ThreadLocalRandom random) {
        List<String> pool = switch (productType) {
            case "home"   -> cacheDummy.getHomeProductCodes();
            case "mobile" -> cacheDummy.getMobileProductCodes();
            default       -> cacheDummy.getAdditionalProductCodes();
        };
        return pool.get(random.nextInt(pool.size()));
    }

    private String pickDifferentCode(String productType, String exclude, ThreadLocalRandom random) {
        List<String> pool = switch (productType) {
            case "home"   -> cacheDummy.getHomeProductCodes();
            case "mobile" -> cacheDummy.getMobileProductCodes();
            default       -> cacheDummy.getAdditionalProductCodes();
        };
        if (pool.size() == 1) return pool.get(0);
        String code;
        do { code = pool.get(random.nextInt(pool.size())); } while (code.equals(exclude));
        return code;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  유틸리티
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * iam_memo — 아웃바운드 통화 결과를 기록한다.
     * 한글 평문으로 결과를 기술하고 맨 뒤 괄호 안에 카테고리 코드를 표기한다.
     * <ul>
     *   <li>CONVERTED: 방어 성공 내용을 한글로 기술 {@code (<defenseCategory>)}</li>
     *   <li>REJECTED:  거절 사유를 한글로 기술 {@code (<outboundCategory>)}</li>
     * </ul>
     */
    private String buildMemo(OutboundTextResult textResult) {
        if ("CONVERTED".equals(textResult.callResult())) {
            return buildConvertedMemo(textResult.defenseCategory());
        }
        return buildRejectedMemo(textResult.outboundCategory());
    }

    private String buildConvertedMemo(String defenseCategory) {
        if (defenseCategory == null) return "상담 결과 고객 전환 성공 (CONVERTED)";
        return switch (defenseCategory) {
            case "BNFT_DISCOUNT"    -> "요금 할인 혜택 제안으로 고객 재약정 전환 완료 (BNFT_DISCOUNT)";
            case "BNFT_GIFT"        -> "사은품 및 추가 혜택 제공으로 고객 유지 성공 (BNFT_GIFT)";
            case "OPT_DOWNGRADE"    -> "요금제 하향 조정 제안으로 고객 해지 의사 철회 (OPT_DOWNGRADE)";
            case "PHYS_RELOCATION"  -> "이사 지역 서비스 연계 안내로 고객 전환 완료 (PHYS_RELOCATION)";
            case "PHYS_TECH_CHECK"  -> "기술 점검 및 품질 개선 조치 후 고객 유지 (PHYS_TECH_CHECK)";
            case "ADM_GUIDE"        -> "위약금 및 계약 조건 상세 안내 후 고객 동의 확보 (ADM_GUIDE)";
            case "PLAN_CHANGE"      -> "맞춤형 요금제 변경 제안으로 고객 잔류 (PLAN_CHANGE)";
            case "CONTRACT_RENEW"   -> "재약정 조건 안내 후 고객 계약 갱신 완료 (CONTRACT_RENEW)";
            default                 -> "상담 목표 달성, 고객 전환 완료 (" + defenseCategory + ")";
        };
    }

    private String buildRejectedMemo(String outboundCategory) {
        if (outboundCategory == null) return "상담 목표 달성 실패 (REJECTED)";
        return switch (outboundCategory) {
            case "COST"        -> "고객 요금 부담 사유로 재약정 거절 (COST)";
            case "SWITCH"      -> "타사 이동 의사 확고, 해지 방어 실패 (SWITCH)";
            case "DISSATISFIED"-> "서비스 불만으로 인한 재약정 거절 (DISSATISFIED)";
            case "NO_NEED"     -> "고객 서비스 필요 없다고 판단하여 거절 (NO_NEED)";
            case "CONSIDER"    -> "고객 추후 검토 의사 표명, 즉시 전환 미확정 (CONSIDER)";
            case "OTHER"       -> "기타 사유로 상담 목표 달성 실패 (OTHER)";
            default            -> "상담 목표 달성 실패 (" + outboundCategory + ")";
        };
    }

    /**
     * outbound_category code_name을 분포에 따라 선택한다.
     *
     * <p>분포 설계 (카테고리 유형별):
     * <ul>
     *   <li>CHN    (50%): CONVERTED 10% / COST 26% / SWITCH 16% / DISSATISFIED 16% / NO_NEED 13% / CONSIDER 10% / OTHER 9%</li>
     *   <li>TRB    (25%): CONVERTED 35% / DISSATISFIED 35% / CONSIDER 14% / SWITCH 10% / OTHER 6%</li>
     *   <li>UPSELL (15%): CONVERTED 60% / COST 20% / CONSIDER 10% / NO_NEED 7% / OTHER 3%</li>
     *   <li>ARREARS(10%): CONVERTED 75% / CONSIDER 12% / NO_NEED 8% / OTHER 5%</li>
     * </ul>
     *
     * @param categoryType "CHN" | "TRB" | "UPSELL" | "ARREARS"
     * @param roll         0~99 난수
     * @return "CONVERTED" 또는 analysis_code.outbound_category code_name
     */
    private String pickOutboundCategoryKey(String categoryType, int roll) {
        return switch (categoryType) {
            case "UPSELL"   -> pickUpsellOutboundCategoryKey(roll);
            case "ARREARS"  -> pickArrearsOutboundCategoryKey(roll);
            case "TRB"      -> pickTrbOutboundCategoryKey(roll);
            case "NEW_SUBS" -> pickNewSubsOutboundCategoryKey(roll);
            default         -> pickChnOutboundCategoryKey(roll);
        };
    }

    /** CHN: CONVERTED 10% / COST 26% / SWITCH 16% / DISSATISFIED 16% / NO_NEED 13% / CONSIDER 10% / OTHER 9% */
    private String pickChnOutboundCategoryKey(int roll) {
        if (roll < 10) return "CONVERTED";
        if (roll < 36) return "COST";
        if (roll < 52) return "SWITCH";
        if (roll < 68) return "DISSATISFIED";
        if (roll < 81) return "NO_NEED";
        if (roll < 91) return "CONSIDER";
        return "OTHER";
    }

    /** UPSELL: CONVERTED 55% / COST 22% / CONSIDER 12% / NO_NEED 7% / OTHER 4% */
    private String pickUpsellOutboundCategoryKey(int roll) {
        if (roll < 55) return "CONVERTED";
        if (roll < 77) return "COST";
        if (roll < 89) return "CONSIDER";
        if (roll < 96) return "NO_NEED";
        return "OTHER";
    }

    /** ARREARS: CONVERTED 75% / CONSIDER 12% / NO_NEED 8% / OTHER 5% */
    private String pickArrearsOutboundCategoryKey(int roll) {
        if (roll < 75) return "CONVERTED";
        if (roll < 87) return "CONSIDER";
        if (roll < 95) return "NO_NEED";
        return "OTHER";
    }

    /** TRB: CONVERTED 45% / DISSATISFIED 27% / CONSIDER 14% / SWITCH 9% / OTHER 5% */
    private String pickTrbOutboundCategoryKey(int roll) {
        if (roll < 45) return "CONVERTED";
        if (roll < 72) return "DISSATISFIED";
        if (roll < 86) return "CONSIDER";
        if (roll < 95) return "SWITCH";
        return "OTHER";
    }

    /** NEW_SUBS: CONVERTED 60% / NO_NEED 20% / CONSIDER 13% / OTHER 7% */
    private String pickNewSubsOutboundCategoryKey(int roll) {
        if (roll < 60) return "CONVERTED";
        if (roll < 80) return "NO_NEED";
        if (roll < 93) return "CONSIDER";
        return "OTHER";
    }

    /** 카테고리 유형 선택: CHN 30% / NEW_SUBS 30% / UPSELL 20% / ARREARS 10% / TRB 10% */
    private String pickCategoryType(int roll) {
        if (roll < 30) return "CHN";
        if (roll < 60) return "NEW_SUBS";
        if (roll < 80) return "UPSELL";
        if (roll < 90) return "ARREARS";
        return "TRB";
    }

    private LocalDateTime randomBusinessTime(LocalDate targetDate, ThreadLocalRandom random) {
        int hour   = 8 + random.nextInt(10); // 08 ~ 17시
        int minute = random.nextInt(60);
        int second = random.nextInt(60);
        return targetDate.atTime(hour, minute, second);
    }
}
