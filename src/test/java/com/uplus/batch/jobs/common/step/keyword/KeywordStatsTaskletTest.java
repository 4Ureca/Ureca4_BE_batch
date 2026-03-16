package com.uplus.batch.jobs.common.step.keyword;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.IntStream;

import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.*;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

@ExtendWith(MockitoExtension.class)
@DisplayName("KeywordStatsTasklet 단위 테스트")
class KeywordStatsTaskletTest {

    @Mock MongoTemplate mongoTemplate;

    private KeywordStatsTasklet tasklet;
    private StepContribution contribution;
    private ChunkContext chunkContext;

    @BeforeEach
    void setUp() {
        tasklet = new KeywordStatsTasklet(mongoTemplate, "weekly_report_snapshot");
    }

    // ==================== 헬퍼 메서드 ====================

    /** 날짜 파라미터가 포함된 ChunkContext 생성 */
    private void initContext(String startDate, String endDate) {
        JobParameters params = new JobParametersBuilder()
                .addString("startDate", startDate)
                .addString("endDate", endDate)
                .toJobParameters();
        JobExecution jobExecution = new JobExecution(1L, params);
        StepExecution stepExecution = new StepExecution("keywordStatsStep", jobExecution);
        contribution = new StepContribution(stepExecution);
        chunkContext = new ChunkContext(new StepContext(stepExecution));
    }

    /** 기본(파라미터 없는) ChunkContext 생성 */
    private void initDefaultContext() {
        JobExecution jobExecution = new JobExecution(1L, new JobParameters());
        StepExecution stepExecution = new StepExecution("keywordStatsStep", jobExecution);
        contribution = new StepContribution(stepExecution);
        chunkContext = new ChunkContext(new StepContext(stepExecution));
    }

    /** topKeywords만 있는 daily_report_snapshot Document 생성 */
    private Document createSnapshot(LocalDate date, Map<String, Integer> keywordCounts) {
        List<Document> topKeywords = new ArrayList<>();
        keywordCounts.forEach((keyword, count) ->
                topKeywords.add(new Document("keyword", keyword).append("count", count)));

        Document keywordSummary = new Document("topKeywords", topKeywords);
        return new Document("date", date).append("keywordSummary", keywordSummary);
    }

    /** topKeywords + byCustomerType 포함 Document 생성 */
    private Document createSnapshotWithGrade(LocalDate date,
                                              Map<String, Integer> keywordCounts,
                                              Map<String, Map<String, Integer>> gradeKeywords) {
        Document snapshot = createSnapshot(date, keywordCounts);

        List<Document> byCustomerType = new ArrayList<>();
        gradeKeywords.forEach((customerType, keywords) -> {
            List<Document> kwDocs = new ArrayList<>();
            keywords.forEach((kw, cnt) ->
                    kwDocs.add(new Document("keyword", kw).append("count", cnt)));
            byCustomerType.add(new Document("customerType", customerType).append("keywords", kwDocs));
        });

        // keywordSummary 안에 byCustomerType 추가
        Document keywordSummary = snapshot.get("keywordSummary", Document.class);
        keywordSummary.append("byCustomerType", byCustomerType);
        return snapshot;
    }

    /** upsert 호출 시 Update 캡처 → keywordSummary Document 추출 */
    private Document captureKeywordSummary() {
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).upsert(any(Query.class), updateCaptor.capture(), eq("weekly_report_snapshot"));

        Document updateDoc = updateCaptor.getValue().getUpdateObject();
        Document setDoc = updateDoc.get("$set", Document.class);
        return setDoc.get("keywordSummary", Document.class);
    }

    // ==================== 테스트 그룹 ====================

    @Nested
    @DisplayName("1. 기본 키워드 집계")
    class BasicAggregation {

        @Test
        @DisplayName("스냅샷 3건 → topKeywords 합산 후 내림차순 순위")
        void 정상데이터_topKeywords_합산_및_순위() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            List<Document> currentSnapshots = List.of(
                    createSnapshot(LocalDate.of(2025, 1, 8), Map.of("해지", 10, "요금제", 5)),
                    createSnapshot(LocalDate.of(2025, 1, 9), Map.of("해지", 15, "요금제", 8, "번호이동", 3)),
                    createSnapshot(LocalDate.of(2025, 1, 10), Map.of("해지", 5, "번호이동", 7))
            );

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(currentSnapshots)   // 현재 기간
                    .thenReturn(List.of())           // 이전 기간
                    .thenReturn(currentSnapshots);   // 28일 lookback

            // when
            RepeatStatus status = tasklet.execute(contribution, chunkContext);

            // then
            assertThat(status).isEqualTo(RepeatStatus.FINISHED);

            Document kwSummary = captureKeywordSummary();
            List<Document> topKeywords = kwSummary.getList("topKeywords", Document.class);

            assertThat(topKeywords).isNotEmpty();
            // 해지: 10+15+5=30, 요금제: 5+8=13, 번호이동: 3+7=10
            assertThat(topKeywords.get(0).getString("keyword")).isEqualTo("해지");
            assertThat(topKeywords.get(0).get("count")).isEqualTo(30L);
            assertThat(topKeywords.get(0).getInteger("rank")).isEqualTo(1);

            assertThat(topKeywords.get(1).getString("keyword")).isEqualTo("요금제");
            assertThat(topKeywords.get(1).get("count")).isEqualTo(13L);
            assertThat(topKeywords.get(1).getInteger("rank")).isEqualTo(2);

            assertThat(topKeywords.get(2).getString("keyword")).isEqualTo("번호이동");
            assertThat(topKeywords.get(2).get("count")).isEqualTo(10L);
            assertThat(topKeywords.get(2).getInteger("rank")).isEqualTo(3);
        }

        @Test
        @DisplayName("키워드 25개 → 상위 20개만 결과에 포함")
        void TOP20_제한_검증() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            Map<String, Integer> manyKeywords = new LinkedHashMap<>();
            for (int i = 1; i <= 25; i++) {
                manyKeywords.put("키워드" + i, 100 - i); // 99, 98, ..., 75
            }
            List<Document> currentSnapshots = List.of(
                    createSnapshot(LocalDate.of(2025, 1, 8), manyKeywords));

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(currentSnapshots)
                    .thenReturn(List.of())
                    .thenReturn(currentSnapshots);

            // when
            tasklet.execute(contribution, chunkContext);

            // then
            Document kwSummary = captureKeywordSummary();
            List<Document> topKeywords = kwSummary.getList("topKeywords", Document.class);
            assertThat(topKeywords).hasSize(20);
        }
    }

    @Nested
    @DisplayName("2. 증감율 계산")
    class ChangeRateCalculation {

        @Test
        @DisplayName("현재 100건, 이전 50건 → changeRate = +100.0%")
        void 이전기간대비_증감율_계산() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            List<Document> current = List.of(
                    createSnapshot(LocalDate.of(2025, 1, 8), Map.of("해지", 100)));
            List<Document> prev = List.of(
                    createSnapshot(LocalDate.of(2025, 1, 1), Map.of("해지", 50)));

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(current)   // 현재
                    .thenReturn(prev)      // 이전
                    .thenReturn(current);  // 28일 lookback

            // when
            tasklet.execute(contribution, chunkContext);

            // then
            Document kwSummary = captureKeywordSummary();
            List<Document> topKeywords = kwSummary.getList("topKeywords", Document.class);
            assertThat(topKeywords.get(0).getDouble("changeRate")).isEqualTo(100.0);
        }

        @Test
        @DisplayName("이전 기간에 키워드 없으면 changeRate = 0")
        void 이전기간_키워드없으면_증감율_0() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            List<Document> current = List.of(
                    createSnapshot(LocalDate.of(2025, 1, 8), Map.of("해지", 50)));

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(current)    // 현재
                    .thenReturn(List.of())  // 이전 (비어있음)
                    .thenReturn(current);   // 28일 lookback

            // when
            tasklet.execute(contribution, chunkContext);

            // then
            Document kwSummary = captureKeywordSummary();
            List<Document> topKeywords = kwSummary.getList("topKeywords", Document.class);
            assertThat(topKeywords.get(0).getDouble("changeRate")).isEqualTo(0.0);
        }

        @Test
        @DisplayName("현재 30건, 이전 60건 → changeRate = -50.0%")
        void 감소시_음수_증감율() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            List<Document> current = List.of(
                    createSnapshot(LocalDate.of(2025, 1, 8), Map.of("해지", 30)));
            List<Document> prev = List.of(
                    createSnapshot(LocalDate.of(2025, 1, 1), Map.of("해지", 60)));

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(current)
                    .thenReturn(prev)
                    .thenReturn(current);

            // when
            tasklet.execute(contribution, chunkContext);

            // then
            Document kwSummary = captureKeywordSummary();
            List<Document> topKeywords = kwSummary.getList("topKeywords", Document.class);
            assertThat(topKeywords.get(0).getDouble("changeRate")).isEqualTo(-50.0);
        }

        @Test
        @DisplayName("현재 7건, 이전 3건 → changeRate = 133.3% (소수 첫째자리 반올림)")
        void 소수점_반올림() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            List<Document> current = List.of(
                    createSnapshot(LocalDate.of(2025, 1, 8), Map.of("해지", 7)));
            List<Document> prev = List.of(
                    createSnapshot(LocalDate.of(2025, 1, 1), Map.of("해지", 3)));

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(current)
                    .thenReturn(prev)
                    .thenReturn(current);

            // when
            tasklet.execute(contribution, chunkContext);

            // then
            Document kwSummary = captureKeywordSummary();
            List<Document> topKeywords = kwSummary.getList("topKeywords", Document.class);
            // (7-3)*100.0/3 = 133.333... → Math.round(1333.33)/10.0 = 133.3
            assertThat(topKeywords.get(0).getDouble("changeRate")).isEqualTo(133.3);
        }
    }

    @Nested
    @DisplayName("3. 장기 상위 유지 키워드")
    class LongTermKeywords {

        @Test
        @DisplayName("28일 중 5일 TOP10 등장 → 장기 키워드로 분류")
        void 일4일이상_등장시_장기키워드() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            // 현재 기간 스냅샷 (1건)
            List<Document> current = List.of(
                    createSnapshot(LocalDate.of(2025, 1, 8), Map.of("해지", 50)));

            // 28일 lookback 스냅샷 — "해지"가 5일 등장
            List<Document> longTermSnapshots = new ArrayList<>();
            for (int i = 0; i < 28; i++) {
                Map<String, Integer> keywords = new HashMap<>();
                if (i < 5) {
                    keywords.put("해지", 10);  // 5일 등장
                }
                keywords.put("기타키워드" + i, 5); // 매일 다른 키워드
                longTermSnapshots.add(createSnapshot(LocalDate.of(2024, 12, 18).plusDays(i), keywords));
            }

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(current)           // 현재 기간
                    .thenReturn(List.of())          // 이전 기간
                    .thenReturn(longTermSnapshots);  // 28일 lookback

            // when
            tasklet.execute(contribution, chunkContext);

            // then
            Document kwSummary = captureKeywordSummary();
            List<Document> longTermTopKeywords = kwSummary.getList("longTermTopKeywords", Document.class);

            assertThat(longTermTopKeywords).isNotEmpty();
            Document target = longTermTopKeywords.stream()
                    .filter(d -> "해지".equals(d.getString("keyword")))
                    .findFirst().orElse(null);
            assertThat(target).isNotNull();
            assertThat(target.getInteger("appearanceDays")).isEqualTo(5);
            assertThat(target.getInteger("totalDays")).isEqualTo(28);
        }

        @Test
        @DisplayName("28일 중 3일 등장 → LONG_TERM_THRESHOLD(4) 미달로 제외")
        void 일3일_등장시_제외() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            List<Document> current = List.of(
                    createSnapshot(LocalDate.of(2025, 1, 8), Map.of("해지", 50)));

            // 28일 lookback — "해지"가 3일만 등장
            List<Document> longTermSnapshots = new ArrayList<>();
            for (int i = 0; i < 28; i++) {
                Map<String, Integer> keywords = new HashMap<>();
                if (i < 3) {
                    keywords.put("해지", 10);
                }
                keywords.put("기타키워드" + i, 5);
                longTermSnapshots.add(createSnapshot(LocalDate.of(2024, 12, 18).plusDays(i), keywords));
            }

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(current)
                    .thenReturn(List.of())
                    .thenReturn(longTermSnapshots);

            // when
            tasklet.execute(contribution, chunkContext);

            // then
            Document kwSummary = captureKeywordSummary();
            List<Document> longTermTopKeywords = kwSummary.getList("longTermTopKeywords", Document.class);

            // "해지"는 3일만 등장 → 장기 키워드에 포함되지 않아야 함
            boolean hasHaeji = longTermTopKeywords.stream()
                    .anyMatch(d -> "해지".equals(d.getString("keyword")));
            assertThat(hasHaeji).isFalse();
        }

        @Test
        @DisplayName("여러 장기 키워드 → appearanceDays 기준 내림차순 정렬 및 rank 할당")
        void 등장일수_내림차순_정렬() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            List<Document> current = List.of(
                    createSnapshot(LocalDate.of(2025, 1, 8), Map.of("해지", 50, "요금제", 30)));

            // 28일 lookback — "해지" 20일, "요금제" 16일
            List<Document> longTermSnapshots = new ArrayList<>();
            for (int i = 0; i < 28; i++) {
                Map<String, Integer> keywords = new HashMap<>();
                if (i < 20) keywords.put("해지", 10);
                if (i < 16) keywords.put("요금제", 8);
                keywords.put("기타" + i, 1);
                longTermSnapshots.add(createSnapshot(LocalDate.of(2024, 12, 18).plusDays(i), keywords));
            }

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(current)
                    .thenReturn(List.of())
                    .thenReturn(longTermSnapshots);

            // when
            tasklet.execute(contribution, chunkContext);

            // then
            Document kwSummary = captureKeywordSummary();
            List<Document> longTermTopKeywords = kwSummary.getList("longTermTopKeywords", Document.class);

            assertThat(longTermTopKeywords).hasSizeGreaterThanOrEqualTo(2);
            // rank1 = 해지(20일), rank2 = 요금제(16일)
            assertThat(longTermTopKeywords.get(0).getString("keyword")).isEqualTo("해지");
            assertThat(longTermTopKeywords.get(0).getInteger("rank")).isEqualTo(1);
            assertThat(longTermTopKeywords.get(0).getInteger("appearanceDays")).isEqualTo(20);

            assertThat(longTermTopKeywords.get(1).getString("keyword")).isEqualTo("요금제");
            assertThat(longTermTopKeywords.get(1).getInteger("rank")).isEqualTo(2);
            assertThat(longTermTopKeywords.get(1).getInteger("appearanceDays")).isEqualTo(16);
        }
    }

    @Nested
    @DisplayName("4. 고객 유형별 키워드")
    class CustomerTypeKeywords {

        @Test
        @DisplayName("VIP 등급에 키워드 7개 → 상위 5개만 추출")
        void 등급별_TOP5_추출() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            Map<String, Integer> topKw = Map.of("해지", 10);
            Map<String, Map<String, Integer>> gradeKw = Map.of(
                    "VIP", new LinkedHashMap<>(Map.of(
                            "해지", 50, "요금제", 40, "번호이동", 35, "위약금", 30,
                            "기기변경", 25, "데이터", 20, "로밍", 15
                    ))
            );

            List<Document> current = List.of(
                    createSnapshotWithGrade(LocalDate.of(2025, 1, 8), topKw, gradeKw));

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(current)
                    .thenReturn(List.of())
                    .thenReturn(current);

            // when
            tasklet.execute(contribution, chunkContext);

            // then
            Document kwSummary = captureKeywordSummary();
            List<Document> byCustomerType = kwSummary.getList("byCustomerType", Document.class);

            assertThat(byCustomerType).hasSize(1);
            Document vip = byCustomerType.get(0);
            assertThat(vip.getString("customerType")).isEqualTo("VIP");
            List<Document> keywords = vip.getList("keywords", Document.class);
            assertThat(keywords).hasSize(5);
        }

        @Test
        @DisplayName("VIP, DIAMOND 등급 → 개별 CustomerTypeKeyword 생성")
        void 다중등급_각각_집계() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            Map<String, Integer> topKw = Map.of("해지", 10);
            Map<String, Map<String, Integer>> gradeKw = Map.of(
                    "VIP", Map.of("해지", 50, "요금제", 30),
                    "DIAMOND", Map.of("기기변경", 20, "위약금", 15)
            );

            List<Document> current = List.of(
                    createSnapshotWithGrade(LocalDate.of(2025, 1, 8), topKw, gradeKw));

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(current)
                    .thenReturn(List.of())
                    .thenReturn(current);

            // when
            tasklet.execute(contribution, chunkContext);

            // then
            Document kwSummary = captureKeywordSummary();
            List<Document> byCustomerType = kwSummary.getList("byCustomerType", Document.class);

            assertThat(byCustomerType).hasSize(2);
            Set<String> customerTypes = new HashSet<>();
            byCustomerType.forEach(ct -> customerTypes.add(ct.getString("customerType")));
            assertThat(customerTypes).containsExactlyInAnyOrder("VIP", "DIAMOND");
        }
    }

    @Nested
    @DisplayName("5. MongoDB upsert 검증")
    class UpsertVerification {

        @Test
        @DisplayName("weekly_report_snapshot 컬렉션에 upsert")
        void upsert_컬렉션명_검증() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            List<Document> current = List.of(
                    createSnapshot(LocalDate.of(2025, 1, 8), Map.of("해지", 10)));

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(current)
                    .thenReturn(List.of())
                    .thenReturn(current);

            // when
            tasklet.execute(contribution, chunkContext);

            // then
            verify(mongoTemplate).upsert(any(Query.class), any(Update.class), eq("weekly_report_snapshot"));
        }

        @Test
        @DisplayName("Update에 topKeywords, longTermTopKeywords, byCustomerType 포함")
        void upsert_keywordSummary_구조() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            List<Document> current = List.of(
                    createSnapshot(LocalDate.of(2025, 1, 8), Map.of("해지", 10)));

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(current)
                    .thenReturn(List.of())
                    .thenReturn(current);

            // when
            tasklet.execute(contribution, chunkContext);

            // then
            Document kwSummary = captureKeywordSummary();
            assertThat(kwSummary).containsKey("topKeywords");
            assertThat(kwSummary).containsKey("longTermTopKeywords");
            assertThat(kwSummary).containsKey("byCustomerType");
        }
    }

    @Nested
    @DisplayName("6. 예외 및 빈 데이터")
    class EdgeCases {

        @Test
        @DisplayName("현재 기간 스냅샷 0건 → upsert 미호출, FINISHED 반환")
        void 현재기간_스냅샷없음_즉시완료() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(List.of());

            // when
            RepeatStatus status = tasklet.execute(contribution, chunkContext);

            // then
            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
            verify(mongoTemplate, never()).upsert(any(), any(Update.class), anyString());
        }

        @Test
        @DisplayName("스냅샷에 topKeywords 필드 null → NPE 없이 정상 처리")
        void topKeywords_null_스킵() throws Exception {
            // given
            initContext("2025-01-08", "2025-01-14");

            Document snapshotWithNullTopKeywords = new Document("date", LocalDate.of(2025, 1, 8));
            // topKeywords 필드 없음 (null)

            when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("daily_report_snapshot")))
                    .thenReturn(List.of(snapshotWithNullTopKeywords))
                    .thenReturn(List.of())
                    .thenReturn(List.of(snapshotWithNullTopKeywords));

            // when
            RepeatStatus status = tasklet.execute(contribution, chunkContext);

            // then — NPE 없이 정상 완료
            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
            // topKeywords가 모두 null이라 합산 결과는 비어있음 → upsert에 빈 리스트 저장
            verify(mongoTemplate).upsert(any(Query.class), any(Update.class), eq("weekly_report_snapshot"));
        }
    }
}
