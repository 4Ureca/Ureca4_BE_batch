package com.uplus.batch.domain.summary.service;

import com.uplus.batch.common.dummy.dto.CacheDummy;
import com.uplus.batch.domain.extraction.entity.ConsultationExtraction;
import com.uplus.batch.domain.extraction.repository.ConsultationExtractionRepository;
import com.uplus.batch.domain.summary.entity.SummaryEventStatus;
import com.uplus.batch.domain.summary.repository.SummaryEventStatusRepository;
import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.jobs.summary_dummy.dto.ConsultationResultRow;
import com.uplus.batch.jobs.summary_dummy.generator.ConsultationSummaryDummyGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * SummaryEventStatus(REQUESTED) 감지 → ConsultationSummary 생성 후 MongoDB 저장.
 *
 * <p>처리 흐름:
 * <ol>
 *   <li>SummaryEventStatus REQUESTED 건을 배치 크기만큼 조회</li>
 *   <li>consultation_results + 연관 테이블 조인으로 ConsultationResultRow 로드</li>
 *   <li>ConsultationSummaryDummyGenerator.generate()로 기본 문서 생성</li>
 *   <li>riskFlags / cancellation / iam / summary.content 오버라이드 적용</li>
 *   <li>MongoDB consultation_summary에 저장 (consultId 기준 upsert)</li>
 *   <li>SummaryEventStatus → COMPLETED</li>
 * </ol>
 *
 * <p>summary.content 연결 로직:
 * retention_analysis.raw_summary 조회 시 값이 있으면 COMPLETED 상태로,
 * 없으면 PENDING 상태로 저장한다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ConsultationSummaryGenerator {

    private final SummaryEventStatusRepository summaryEventStatusRepo;
    private final ConsultationExtractionRepository extractionRepo;
    private final ConsultationSummaryDummyGenerator dummyGenerator;
    private final CacheDummy cacheDummy;
    private final JdbcTemplate jdbcTemplate;
    private final MongoTemplate mongoTemplate;

    /** self-injection: processSingleTask의 @Transactional(REQUIRES_NEW)이 AOP 프록시를 통해 적용되도록 */
    @Autowired
    @Lazy
    private ConsultationSummaryGenerator self;

    @Value("${app.summary-sync.batch-size:100}")
    private int batchSize;

    private static final String RESULT_ROW_SQL = """
            SELECT cr.consult_id,
                   cr.created_at,
                   cr.emp_id,
                   e.name        AS agent_name,
                   cr.customer_id,
                   c.name        AS customer_name,
                   c.customer_type,
                   c.phone,
                   c.grade_code,
                   c.birth_date,
                   c.gender,
                   cr.category_code,
                   cp.large_category,
                   cp.medium_category,
                   cp.small_category,
                   cr.channel,
                   cr.duration_sec,
                   cr.iam_issue,
                   cr.iam_action,
                   cr.iam_memo
            FROM consultation_results cr
                     JOIN employees e ON cr.emp_id = e.emp_id
                     JOIN customers c ON cr.customer_id = c.customer_id
                     JOIN consultation_category_policy cp ON cr.category_code = cp.category_code
            WHERE cr.consult_id = ?
            """;

    private static final List<String> COMPLAINT_REASONS = List.of(
            "요금 불만", "서비스 품질 불만", "경쟁사 이동", "약정 만료", "단말기 불만", "부가서비스 불만"
    );

    @Scheduled(cron = "${app.summary-sync.cron}")
    public void processSummaryQueue() {
        List<SummaryEventStatus> pending =
                summaryEventStatusRepo.findTop100ByStatusOrderByCreatedAtAsc(
                        com.uplus.batch.domain.summary.enums.SummaryEventStatus.REQUESTED.getValue());

        if (pending.isEmpty()) {
            log.debug("[SummaryGenerator] 처리할 REQUESTED 건 없음");
            return;
        }

        log.info("[SummaryGenerator] Summary 생성 시작 - {}건", pending.size());

        for (SummaryEventStatus task : pending) {
            try {
                self.processSingleTask(task); // self-injection으로 @Transactional(REQUIRES_NEW) 적용
            } catch (Exception e) {
                log.error("[SummaryGenerator] consultId={} 처리 중 예상치 못한 오류: {}",
                        task.getConsultId(), e.getMessage());
            }
        }

        log.info("[SummaryGenerator] Summary 생성 완료 - {}건 처리", pending.size());
    }

    /**
     * 개별 Summary 생성. REQUIRES_NEW 트랜잭션으로 격리하여 한 건 실패가 다른 건에 영향을 주지 않는다.
     * processSummaryQueue()는 self-injection(self.processSingleTask())으로 호출하여 AOP 프록시 적용.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processSingleTask(SummaryEventStatus task) {
        try {
            task.start();
            summaryEventStatusRepo.saveAndFlush(task);

            // 1. ConsultationResultRow 로드 (MySQL)
            ConsultationResultRow row = loadResultRow(task.getConsultId());

            // 2. ConsultationSummaryDummyGenerator 기본 문서 생성
            ConsultationSummary doc = dummyGenerator.generate(row);

            // 3. 합성 데이터 출처 마킹 (iam.memo의 [SYNTHETIC] 태그 기반)
            if (row.getIamMemo() != null && row.getIamMemo().startsWith("[SYNTHETIC]")) {
                doc.setSource("SYNTHETIC");
            }

            // 4. riskFlags 오버라이드 (40% 위험군, CHURN > FRAUD > PHISHING)
            doc.setRiskFlags(generateRiskFlags());

            // 5. cancellation 오버라이드 (CHN 카테고리일 때만 intent=true 가능)
            doc.setCancellation(buildCancellation(row.getCategoryCode()));

            // 6. iam 오버라이드 (상담사 empId % 3 기반 숙련도로 matchRates 차등 적용)
            doc.setIam(buildIam(row));

            // 7. summary.content 연결 (retention_analysis.raw_summary 조회)
            //    AI 추출 미완료 시 PENDING 상태로 저장, 이후 재처리 고려
            String rawSummary = extractionRepo.findById(task.getConsultId())
                    .map(ConsultationExtraction::getRawSummary)
                    .orElse(null);

            doc.setSummary(buildSummary(rawSummary));

            // 7. MongoDB upsert (consultId 중복 방어)
            mongoTemplate.upsert(
                    new Query(Criteria.where("consultId").is(doc.getConsultId())),
                    buildUpdateFromDoc(doc),
                    ConsultationSummary.class
            );

            task.complete();
            summaryEventStatusRepo.saveAndFlush(task);

            log.info("[SummaryGenerator] consultId={} 완료 (summary.status={})",
                    task.getConsultId(), rawSummary != null ? "COMPLETED" : "PENDING");

        } catch (Exception e) {
            log.error("[SummaryGenerator] consultId={} 실패: {}", task.getConsultId(), e.getMessage());
            task.fail(e.getMessage());
            summaryEventStatusRepo.saveAndFlush(task);
        }
    }

    // ─────────────────────────────────────────────────────
    //  ConsultationResultRow 로드
    // ─────────────────────────────────────────────────────

    private ConsultationResultRow loadResultRow(Long consultId) {
        List<ConsultationResultRow> rows = jdbcTemplate.query(
                RESULT_ROW_SQL,
                (rs, rowNum) -> new ConsultationResultRow(
                        rs.getLong("consult_id"),
                        rs.getTimestamp("created_at").toLocalDateTime(),
                        rs.getLong("emp_id"),
                        rs.getString("agent_name"),
                        rs.getLong("customer_id"),
                        rs.getString("customer_name"),
                        rs.getString("customer_type"),
                        rs.getString("phone"),
                        rs.getString("grade_code"),
                        rs.getDate("birth_date") != null
                                ? rs.getDate("birth_date").toLocalDate() : null,
                        rs.getString("gender"),
                        rs.getString("category_code"),
                        rs.getString("large_category"),
                        rs.getString("medium_category"),
                        rs.getString("small_category"),
                        rs.getString("channel"),
                        rs.getInt("duration_sec"),
                        rs.getString("iam_issue"),
                        rs.getString("iam_action"),
                        rs.getString("iam_memo")
                ),
                consultId
        );

        if (rows.isEmpty()) {
            throw new RuntimeException("consultation_results에서 consultId=" + consultId + " 를 찾을 수 없음");
        }
        return rows.get(0);
    }

    // ─────────────────────────────────────────────────────
    //  오버라이드 필드 생성 로직
    // ─────────────────────────────────────────────────────

    /**
     * riskFlags 생성 (§2-3 분포):
     * - 전체의 40%에 riskFlag 부여
     * - 위험군 내 빈도: CHURN > FRAUD > PHISHING 순
     * - 1건에 복수 flag 가능
     */
    private List<String> generateRiskFlags() {
        Random random = ThreadLocalRandom.current();
        if (random.nextInt(100) >= 40) {
            return null; // 60% riskFlags 없음
        }

        List<String> flags = new ArrayList<>();
        if (random.nextInt(100) < 60) flags.add("CHURN");     // 가장 빈도 높음
        if (random.nextInt(100) < 30) flags.add("FRAUD");     // 두 번째
        if (random.nextInt(100) < 20) flags.add("PHISHING");  // 세 번째
        if (random.nextInt(100) < 10) flags.add("ABUSE");
        if (random.nextInt(100) < 5)  flags.add("POLICY");

        if (flags.isEmpty()) flags.add("CHURN"); // 위험군이면 최소 1개 보장
        return flags;
    }

    /**
     * cancellation 생성 (§3-2):
     * - CHN 카테고리일 때만 intent=true 가능 (50% 확률)
     * - 나머지 카테고리는 intent=false
     */
    private ConsultationSummary.Cancellation buildCancellation(String categoryCode) {
        Random random = ThreadLocalRandom.current();
        boolean isChn = categoryCode != null && categoryCode.contains("CHN");
        boolean intent = isChn && random.nextInt(100) < 50;

        if (!intent) {
            return ConsultationSummary.Cancellation.builder()
                    .intent(false)
                    .defenseAttempted(false)
                    .defenseSuccess(false)
                    .build();
        }

        boolean defenseAttempted = random.nextBoolean();
        boolean defenseSuccess = defenseAttempted && random.nextInt(100) < 50;

        List<String> defenseActions = null;
        if (defenseAttempted && cacheDummy.getDefenseActions() != null
                && !cacheDummy.getDefenseActions().isEmpty()) {
            defenseActions = List.of(
                    cacheDummy.getDefenseActions()
                            .get(random.nextInt(cacheDummy.getDefenseActions().size()))
            );
        }

        return ConsultationSummary.Cancellation.builder()
                .intent(true)
                .defenseAttempted(defenseAttempted)
                .defenseSuccess(defenseSuccess)
                .defenseActions(defenseActions)
                .complaintReasons(COMPLAINT_REASONS.get(random.nextInt(COMPLAINT_REASONS.size())))
                .build();
    }

    /**
     * iam 생성 — empId % 3 으로 숙련도 분류:
     * 0=LOW(0.2~0.4), 1=NORMAL(0.4~0.7), 2=HIGH(0.7~0.95)
     *
     * 실제 운영 시 employee_details.skill_level 컬럼으로 대체 권장.
     */
    private ConsultationSummary.Iam buildIam(ConsultationResultRow row) {
        Random random = ThreadLocalRandom.current();
        int tier = (int) (row.getEmpId() % 3);
        double minRate, maxRate;
        switch (tier) {
            case 0  -> { minRate = 0.2; maxRate = 0.4; }  // LOW
            case 2  -> { minRate = 0.7; maxRate = 0.95; } // HIGH
            default -> { minRate = 0.4; maxRate = 0.7; }  // NORMAL
        }
        double matchRates = minRate + random.nextDouble() * (maxRate - minRate);

        return ConsultationSummary.Iam.builder()
                .issue(row.getIamIssue())
                .action(row.getIamAction())
                .memo(row.getIamMemo())
                .matchKeyword(List.of("요금", "할인", "키워드"))
                .matchRates(Math.round(matchRates * 100.0) / 100.0)
                .build();
    }

    /**
     * summary 필드 생성 — rawSummary 유무에 따라 COMPLETED / PENDING 분기.
     * rawSummary가 null이면 AI 추출이 아직 완료되지 않은 것으로, PENDING으로 저장.
     */
    private ConsultationSummary.Summary buildSummary(String rawSummary) {
        if (rawSummary != null && !rawSummary.isBlank()) {
            return ConsultationSummary.Summary.builder()
                    .status("COMPLETED")
                    .content(rawSummary)
                    .keywords(List.of())
                    .build();
        }
        return ConsultationSummary.Summary.builder()
                .status("PENDING")
                .content(null)
                .keywords(List.of())
                .build();
    }

    /**
     * MongoDB에서 summary.status=PENDING인 문서를 재확인.
     * AI 추출이 완료(retention_analysis.raw_summary 생성)되었으면 COMPLETED로 업데이트.
     * processSummaryQueue()와 동일한 cron으로 실행 (매 1분).
     */
    @Scheduled(cron = "${app.summary-sync.cron}")
    public void retryPendingSummaries() {
        Query pendingQuery = new Query(Criteria.where("summary.status").is("PENDING")).limit(50);
        List<ConsultationSummary> pendingDocs = mongoTemplate.find(pendingQuery, ConsultationSummary.class);

        if (pendingDocs.isEmpty()) {
            return;
        }

        log.info("[SummaryGenerator] PENDING 재처리 시작 - {}건", pendingDocs.size());
        int resolved = 0;
        for (ConsultationSummary doc : pendingDocs) {
            String rawSummary = extractionRepo.findById(doc.getConsultId())
                    .map(ConsultationExtraction::getRawSummary)
                    .filter(s -> s != null && !s.isBlank())
                    .orElse(null);

            if (rawSummary != null) {
                mongoTemplate.updateFirst(
                        new Query(Criteria.where("consultId").is(doc.getConsultId())),
                        new Update().set("summary.status", "COMPLETED").set("summary.content", rawSummary),
                        ConsultationSummary.class
                );
                resolved++;
            }
        }
        log.info("[SummaryGenerator] PENDING 재처리 완료 - {}건 → COMPLETED", resolved);
    }

    /**
     * ConsultationSummary 객체를 MongoDB Update 문서로 변환.
     * consultId 기준 upsert에 사용한다.
     */
    private Update buildUpdateFromDoc(ConsultationSummary doc) {
        Update update = new Update();
        update.set("consultId",    doc.getConsultId());
        update.set("consultedAt",  doc.getConsultedAt());
        update.set("channel",      doc.getChannel());
        update.set("durationSec",  doc.getDurationSec());
        update.set("agent",        doc.getAgent());
        update.set("customer",     doc.getCustomer());
        update.set("category",     doc.getCategory());
        update.set("iam",          doc.getIam());
        update.set("riskFlags",    doc.getRiskFlags());
        update.set("cancellation", doc.getCancellation());
        update.set("resultProducts", doc.getResultProducts());
        update.set("summary",      doc.getSummary());
        update.set("source",       doc.getSource());  // null = 실 운영, "SYNTHETIC" = 합성
        update.setOnInsert("createdAt", LocalDateTime.now());
        return update;
    }
}
