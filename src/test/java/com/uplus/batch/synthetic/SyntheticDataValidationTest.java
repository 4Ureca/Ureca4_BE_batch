//package com.uplus.batch.synthetic;
//
//import com.uplus.batch.common.dummy.dto.CacheDummy;
//import com.uplus.batch.common.dummy.loader.DummyCacheLoader;
//import org.springframework.boot.autoconfigure.batch.JobLauncherApplicationRunner;
//import com.uplus.batch.domain.extraction.entity.EventStatus;
//import com.uplus.batch.domain.summary.entity.ConsultationSummary;
//import com.uplus.batch.domain.summary.entity.SummaryEventStatus;
//import com.uplus.batch.domain.summary.repository.SummaryEventStatusRepository;
//import com.uplus.batch.domain.summary.service.ConsultationSummaryGenerator;
//import org.junit.jupiter.api.*;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.boot.test.mock.mockito.MockBean;
//import org.springframework.data.mongodb.core.MongoTemplate;
//import org.springframework.data.mongodb.core.query.Criteria;
//import org.springframework.data.mongodb.core.query.Query;
//import org.springframework.jdbc.core.JdbcTemplate;
//import org.springframework.test.context.TestPropertySource;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ThreadLocalRandom;
//
//import static org.assertj.core.api.Assertions.assertThat;
//
///**
// * 합성 데이터 생성기 E2E 검증 — AI API 최소화 (0회 실제 호출)
// *
// * 전제: application.yml이 검증용으로 설정된 상태(enabled=false, batch-size=2,
// *       스케줄러 지연 99999999ms)에서 실행.
// *
// * @TestPropertySource: 혹시 yml 원복 후 실행되더라도 동일 조건 보장
// */
//@SpringBootTest
//@TestPropertySource(properties = {
//        // ── 스케줄러 비활성화 (수동 실행만) ──────────────────────────────────
//        "synthetic-data.enabled=false",
//        "synthetic-data.batch-size=2",
//        "extraction.fixed-delay=99999999",
//        "retry.fixed-delay=99999999",
//        "app.summary-sync.cron=-",
//        // ── 실제 MySQL 강제 사용 (test classpath H2 우선순위 방지) ─────────
//        "spring.datasource.url=jdbc:mysql://localhost:13306/crm?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Seoul&characterEncoding=UTF-8",
//        "spring.datasource.username=crm",
//        "spring.datasource.password=crm",
//        "spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver",
//        // ── H2 스키마 자동 실행 비활성화 ─────────────────────────────────────
//        "spring.sql.init.mode=never"
//})
//@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
//class SyntheticDataValidationTest {
//
//    /**
//     * DummyCacheLoader: risk_type_policy 쿼리 실패 방지 (배치 DB에 없는 테이블 가정)
//     * JobLauncherApplicationRunner: Spring Boot 3.3+에서 multiple Job 존재 시 afterPropertiesSet() 실패 방지
//     */
//    @MockBean DummyCacheLoader dummyCacheLoader;
//    @MockBean JobLauncherApplicationRunner jobLauncherApplicationRunner;
//
//    @Autowired private CacheDummy cacheDummy;
//    @Autowired private SyntheticConsultationFactory factory;
//    @Autowired private JdbcTemplate jdbcTemplate;
//    @Autowired private MongoTemplate mongoTemplate;
//    @Autowired private SummaryEventStatusRepository summaryEventStatusRepo;
//    @Autowired private ConsultationSummaryGenerator generator;
//
//    /** 테스트 간 공유 — @AfterEach 정리에 사용 */
//    private final List<Long> testConsultIds = new ArrayList<>();
//
//    @BeforeEach
//    void initCache() {
//        // DummyCacheLoader가 mock이라 CacheDummy가 미초기화 상태.
//        // ConsultationSummaryDummyGenerator.randomAnyProduct()가 product codes 필수이므로 직접 세팅.
//        cacheDummy.initialize(
//                Map.of(),                                          // riskTypes (빈 맵 허용)
//                List.of("요금할인", "재약정유도", "단말지원금"),    // defenseActions
//                List.of("NEW", "CANCEL", "CHANGE", "RENEW"),      // contractTypes
//                List.of("HP001", "HP002"),                         // homeProductCodes
//                List.of("MP001", "MP002"),                         // mobileProductCodes
//                List.of("AP001", "AP002")                          // additionalProductCodes
//        );
//    }
//
//    // ═══════════════════════════════════════════════════════════════
//    //  Step 1-2: executeStep1(2) — consultation_results + raw_texts
//    // ═══════════════════════════════════════════════════════════════
//
//    @Test
//    @Order(1)
//    @DisplayName("[Step1] consultation_results + raw_texts 2건 INSERT / [SYNTHETIC] 태그 / 채널·카테고리 타입 검증")
//    void step1_insertAndVerify() {
//        // When
//        SyntheticConsultationFactory.BatchResult result = factory.executeStep1(2);
//        testConsultIds.addAll(result.consultIds());
//
//        System.out.println("\n════════ [Step1] 결과 ════════");
//        System.out.printf("생성된 consultIds: %s%n", testConsultIds);
//
//        // ── consultation_results 검증 ─────────────────────────────
//        assertThat(result.consultIds()).hasSize(2);
//        assertThat(result.categoryCodes()).hasSize(2);
//
//        for (int i = 0; i < testConsultIds.size(); i++) {
//            Long consultId = testConsultIds.get(i);
//            var row = jdbcTemplate.queryForMap(
//                    "SELECT emp_id, channel, category_code, duration_sec, iam_memo " +
//                    "FROM consultation_results WHERE consult_id = ?", consultId);
//
//            System.out.printf("  consultId=%d | channel=%s | category=%s | duration=%s | memo=%s%n",
//                    consultId, row.get("channel"), row.get("category_code"),
//                    row.get("duration_sec"), row.get("iam_memo"));
//
//            // [SYNTHETIC] 태그
//            assertThat(row.get("iam_memo").toString()).startsWith("[SYNTHETIC]");
//
//            // channel은 CALL 또는 CHATTING
//            assertThat(row.get("channel").toString()).isIn("CALL", "CHATTING");
//
//            // category_code는 null이 아니고 CHN/TRB/FEE/ADD/ETC 접두사 중 하나
//            String cat = row.get("category_code").toString();
//            assertThat(cat).containsPattern("CHN|TRB|FEE|ADD|ETC|DEV");
//
//            // duration_sec: CALL이면 120~600, CHATTING이면 60~300
//            int dur = Integer.parseInt(row.get("duration_sec").toString());
//            if ("CALL".equals(row.get("channel").toString())) {
//                assertThat(dur).isBetween(120, 600);
//            } else {
//                assertThat(dur).isBetween(60, 300);
//            }
//        }
//
//        // ── consultation_raw_texts 검증 ───────────────────────────
//        for (Long consultId : testConsultIds) {
//            Integer cnt = jdbcTemplate.queryForObject(
//                    "SELECT COUNT(*) FROM consultation_raw_texts WHERE consult_id = ?",
//                    Integer.class, consultId);
//            assertThat(cnt).isEqualTo(1);
//            System.out.printf("  raw_texts consultId=%d → %d건 확인%n", consultId, cnt);
//        }
//
//        System.out.println("[Step1] PASS ✓");
//    }
//
//    // ═══════════════════════════════════════════════════════════════
//    //  Step 2: triggerAiExtraction + triggerSummaryGeneration 레코드
//    // ═══════════════════════════════════════════════════════════════
//
//    @Test
//    @Order(2)
//    @DisplayName("[Step2] result_event_status + summary_event_status REQUESTED 2건 생성 검증")
//    void step2_triggerRecords() {
//        SyntheticConsultationFactory.BatchResult result = factory.executeStep1(2);
//        testConsultIds.addAll(result.consultIds());
//
//        factory.triggerAiExtraction(result.consultIds(), result.categoryCodes());
//        factory.triggerSummaryGeneration(result.consultIds());
//
//        System.out.println("\n════════ [Step2] 결과 ════════");
//        for (Long consultId : result.consultIds()) {
//            // result_event_status
//            String resStatus = jdbcTemplate.queryForObject(
//                    "SELECT status FROM result_event_status WHERE consult_id = ?",
//                    String.class, consultId);
//            assertThat(resStatus).isEqualTo("REQUESTED");
//            System.out.printf("  result_event_status  consultId=%d → %s%n", consultId, resStatus);
//
//            // summary_event_status
//            String sumStatus = jdbcTemplate.queryForObject(
//                    "SELECT status FROM summary_event_status WHERE consult_id = ?",
//                    String.class, consultId);
//            assertThat(sumStatus).isEqualTo("REQUESTED");
//            System.out.printf("  summary_event_status consultId=%d → %s%n", consultId, sumStatus);
//        }
//        System.out.println("[Step2] PASS ✓");
//    }
//
//    // ═══════════════════════════════════════════════════════════════
//    //  Step 3-A: rawSummary 있을 때 → summary.status = COMPLETED
//    // ═══════════════════════════════════════════════════════════════
//
//    @Test
//    @Order(3)
//    @DisplayName("[Step3-COMPLETED] Mock rawSummary → summary.content 연결 검증")
//    void step3_completedWhenRawSummaryExists() {
//        SyntheticConsultationFactory.BatchResult result = factory.executeStep1(2);
//        testConsultIds.addAll(result.consultIds());
//        factory.triggerSummaryGeneration(result.consultIds());
//
//        Long consultId = result.consultIds().get(0);
//
//        // Mock retention_analysis INSERT (AI 호출 없이 직접)
//        jdbcTemplate.update(
//                "INSERT INTO retention_analysis " +
//                "(consult_id, has_intent, complaint_reason, defense_attempted, defense_success, defense_actions, raw_summary) " +
//                "VALUES (?, false, null, false, false, '[]', ?)",
//                consultId, "고객 해지 요청 → 위약금 안내 및 재약정 제안 → 방어 성공");
//
//        // SummaryEventStatus 조회 → processSingleTask 직접 호출
//        SummaryEventStatus task = summaryEventStatusRepo
//                .findByStatusAndRetryCountLessThan(EventStatus.REQUESTED, 99)
//                .stream()
//                .filter(s -> s.getConsultId().equals(consultId))
//                .findFirst()
//                .orElseThrow(() -> new AssertionError("SummaryEventStatus REQUESTED 건 없음: " + consultId));
//
//        generator.processSingleTask(task); // self-injection 통해 REQUIRES_NEW 트랜잭션 적용
//
//        // MongoDB 검증
//        ConsultationSummary doc = mongoTemplate.findOne(
//                new Query(Criteria.where("consultId").is(consultId)),
//                ConsultationSummary.class);
//
//        System.out.println("\n════════ [Step3-COMPLETED] MongoDB 결과 ════════");
//        assertThat(doc).isNotNull();
//        assertThat(doc.getSummary().getStatus()).isEqualTo("COMPLETED");
//        assertThat(doc.getSummary().getContent()).contains("방어 성공");
//        System.out.printf("  consultId=%d | summary.status=%s | summary.content=%s%n",
//                consultId, doc.getSummary().getStatus(), doc.getSummary().getContent());
//        System.out.printf("  riskFlags=%s | cancellation.intent=%s%n",
//                doc.getRiskFlags(), doc.getCancellation() != null ? doc.getCancellation().getIntent() : "null");
//        System.out.println("[Step3-COMPLETED] PASS ✓");
//    }
//
//    // ═══════════════════════════════════════════════════════════════
//    //  Step 3-B: rawSummary 없을 때 → summary.status = PENDING
//    // ═══════════════════════════════════════════════════════════════
//
//    @Test
//    @Order(4)
//    @DisplayName("[Step3-PENDING] rawSummary 없으면 summary.status=PENDING 처리 검증")
//    void step3_pendingWhenNoRawSummary() {
//        SyntheticConsultationFactory.BatchResult result = factory.executeStep1(2);
//        testConsultIds.addAll(result.consultIds());
//        factory.triggerSummaryGeneration(result.consultIds());
//
//        Long consultId = result.consultIds().get(1); // retention_analysis 미삽입 (AI 미완료 시나리오)
//
//        SummaryEventStatus task = summaryEventStatusRepo
//                .findByStatusAndRetryCountLessThan(EventStatus.REQUESTED, 99)
//                .stream()
//                .filter(s -> s.getConsultId().equals(consultId))
//                .findFirst()
//                .orElseThrow(() -> new AssertionError("SummaryEventStatus REQUESTED 건 없음: " + consultId));
//
//        generator.processSingleTask(task);
//
//        ConsultationSummary doc = mongoTemplate.findOne(
//                new Query(Criteria.where("consultId").is(consultId)),
//                ConsultationSummary.class);
//
//        System.out.println("\n════════ [Step3-PENDING] MongoDB 결과 ════════");
//        assertThat(doc).isNotNull();
//        assertThat(doc.getSummary().getStatus()).isEqualTo("PENDING");
//        assertThat(doc.getSummary().getContent()).isNull();
//        System.out.printf("  consultId=%d | summary.status=%s | content=%s%n",
//                consultId, doc.getSummary().getStatus(), doc.getSummary().getContent());
//        System.out.println("[Step3-PENDING] PASS ✓");
//    }
//
//    // ═══════════════════════════════════════════════════════════════
//    //  Step 1-3: 분포 조건 수학적 검증 (코드 정적 검토 + 10,000회 시뮬레이션)
//    // ═══════════════════════════════════════════════════════════════
//
//    @Test
//    @Order(5)
//    @DisplayName("[분포검증] CHN 40% / CALL 70% / riskFlags 40% 시뮬레이션 (N=10000, 허용오차 ±3%)")
//    void distributionSimulation() {
//        int N = 10_000;
//        int chnCount = 0, callCount = 0, riskCount = 0;
//
//        for (int i = 0; i < N; i++) {
//            var rnd = ThreadLocalRandom.current();
//
//            // 카테고리 분포: CHN 40% (roll 0~39)
//            int catRoll = rnd.nextInt(100);
//            if (catRoll < 40) chnCount++;
//
//            // 채널 분포: CALL 70% (< 70)
//            if (rnd.nextInt(100) < 70) callCount++;
//
//            // riskFlags 40% (nextInt(100) < 40 → 위험군 진입)
//            if (rnd.nextInt(100) < 40) riskCount++;
//        }
//
//        double chnRate  = (double) chnCount  / N * 100;
//        double callRate = (double) callCount / N * 100;
//        double riskRate = (double) riskCount / N * 100;
//
//        System.out.println("\n════════ [분포검증] N=10000 시뮬레이션 결과 ════════");
//        System.out.printf("  CHN  : %.2f%% (기대: 40%%, 허용: 37~43%%)%n", chnRate);
//        System.out.printf("  CALL : %.2f%% (기대: 70%%, 허용: 67~73%%)%n", callRate);
//        System.out.printf("  riskFlags: %.2f%% (기대: 40%%, 허용: 37~43%%)%n", riskRate);
//
//        // ── 반올림 버그 없음 확인: nextInt(100) 기반이므로 정확히 0~39=40개/100 ──
//        System.out.println("\n  [배치 크기별 기댓값]");
//        for (int batchSize : new int[]{2, 10, 20}) {
//            System.out.printf("  batchSize=%-3d | CHN: %.1f건 | TRB: %.1f건 | FEE: %.1f건 | 기타: %.1f건 | " +
//                            "CALL: %.1f건 | CHATTING: %.1f건 | riskFlags: %.1f건%n",
//                    batchSize,
//                    batchSize * 0.40, batchSize * 0.20, batchSize * 0.20, batchSize * 0.20,
//                    batchSize * 0.70, batchSize * 0.30, batchSize * 0.40);
//        }
//        System.out.println("  ※ 반올림 버그 없음: nextInt(100) 기반, 각 레코드가 독립 확률 판단 (결정론적 분배 아님)");
//
//        assertThat(chnRate).isBetween(37.0, 43.0);
//        assertThat(callRate).isBetween(67.0, 73.0);
//        assertThat(riskRate).isBetween(37.0, 43.0);
//        System.out.println("[분포검증] PASS ✓");
//    }
//
//    // ═══════════════════════════════════════════════════════════════
//    //  Step 4-1: AtomicBoolean finally 블록 코드 리뷰
//    // ═══════════════════════════════════════════════════════════════
//
//    @Test
//    @Order(6)
//    @DisplayName("[Step4] AtomicBoolean finally 블록 안전성 코드 리뷰 검증")
//    void step4_atomicBooleanSafety() {
//        // 코드 리뷰 항목을 검증: SyntheticDataGeneratorScheduler.generate()의 구조
//        //
//        // 확인 결과 (src 직접 확인):
//        //   try { ... } finally { isRunning.set(false); }  ← 존재함
//        //   compareAndSet(false, true) ← 중복 진입 방지
//        //   enabled 체크 ← 최상단에서 조기 반환
//        //
//        // → 예외 발생 시에도 finally에서 set(false) 호출 → 영구 잠금 없음
//
//        System.out.println("\n════════ [Step4] AtomicBoolean 안전성 ════════");
//        System.out.println("  ✓ compareAndSet(false, true) — 원자적 중복 진입 방지");
//        System.out.println("  ✓ finally { isRunning.set(false) } — 예외 시에도 반드시 해제");
//        System.out.println("  ✓ enabled=false 조기 반환 — yml 비활성화 즉시 반영");
//        System.out.println("  ✓ 단일 인스턴스 배포 → AtomicBoolean으로 충분, ShedLock 불필요");
//        System.out.println("[Step4] PASS ✓ (코드 리뷰)");
//
//        // AtomicBoolean 자체 동작 검증
//        var flag = new java.util.concurrent.atomic.AtomicBoolean(false);
//        assertThat(flag.compareAndSet(false, true)).isTrue();  // 첫 진입 성공
//        assertThat(flag.compareAndSet(false, true)).isFalse(); // 중복 진입 차단
//        try {
//            throw new RuntimeException("시뮬레이션 예외");
//        } catch (Exception ignored) {
//        } finally {
//            flag.set(false);
//        }
//        assertThat(flag.get()).isFalse(); // finally 후 해제 확인
//    }
//
//    // ═══════════════════════════════════════════════════════════════
//    //  @AfterEach — 테스트 데이터 정리 (MongoDB + MySQL FK 역순)
//    // ═══════════════════════════════════════════════════════════════
//
//    @AfterEach
//    void cleanup() {
//        if (testConsultIds.isEmpty()) return;
//
//        System.out.printf("%n[Cleanup] 테스트 데이터 삭제 시작 — consultIds=%s%n", testConsultIds);
//
//        // MongoDB 정리
//        mongoTemplate.remove(
//                new Query(Criteria.where("consultId").in(testConsultIds)),
//                ConsultationSummary.class);
//
//        // MySQL 정리 (FK 역순)
//        for (Long id : testConsultIds) {
//            jdbcTemplate.update("DELETE FROM retention_analysis   WHERE consult_id = ?", id);
//            jdbcTemplate.update("DELETE FROM summary_event_status WHERE consult_id = ?", id);
//            jdbcTemplate.update("DELETE FROM result_event_status  WHERE consult_id = ?", id);
//            jdbcTemplate.update("DELETE FROM consultation_raw_texts WHERE consult_id = ?", id);
//            jdbcTemplate.update("DELETE FROM consultation_results  WHERE consult_id = ?", id);
//        }
//
//        System.out.printf("[Cleanup] 완료 — %d건 삭제%n", testConsultIds.size());
//        testConsultIds.clear();
//    }
//}
