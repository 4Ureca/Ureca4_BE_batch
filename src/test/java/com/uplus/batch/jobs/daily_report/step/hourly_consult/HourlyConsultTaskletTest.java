package com.uplus.batch.jobs.daily_report.step.hourly_consult;

import com.uplus.batch.common.elasticsearch.ElasticsearchAnalyzeService;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

/**
 * HourlyConsultTasklet 단위 테스트
 *
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HourlyConsultTaskletTest {

    @Mock private MongoTemplate mongoTemplate;
    @Mock private ElasticsearchAnalyzeService analyzeService;
    @Mock private StepContribution contribution;
    @Mock private ChunkContext chunkContext;
    @Mock private StepContext stepContext;
    @Mock private StepExecution stepExecution;
    @Mock private JobExecution jobExecution;
    @Mock private AggregationResults<Document> aggregationResults;

    // ✅ @Mock 제거 — makeJdbcStub()으로 테스트별 생성
    // @Mock private JdbcTemplate jdbcTemplate;  // varargs mock 이슈로 제거

    @BeforeEach
    void setUp() {
        // tasklet은 각 테스트에서 makeTasklet()으로 생성
        ExecutionContext executionContext = new ExecutionContext();
        given(contribution.getStepExecution()).willReturn(stepExecution);
        given(stepExecution.getJobExecution()).willReturn(jobExecution);
        given(jobExecution.getExecutionContext()).willReturn(executionContext);
        given(chunkContext.getStepContext()).willReturn(stepContext);
        given(stepContext.getStepExecution()).willReturn(stepExecution);
    }

    // ─────────────────────────────────────────────────────
    //  JdbcTemplate 익명 stub 헬퍼
    //
    //  사용법:
    //    makeJdbcStub(fakeRows)              → 모든 호출에 fakeRows 반환
    //    makeJdbcStub(fakeRows, List.of())   → 1번째는 fakeRows, 2·3번째는 List.of()
    //
    //  Mockito willReturn 체이닝과 동일하게 동작:
    //  호출 횟수가 지정 개수를 초과하면 마지막 결과를 계속 반환
    // ─────────────────────────────────────────────────────

    @SafeVarargs
    private JdbcTemplate makeJdbcStub(
            List<Map<String, Object>> first,
            List<Map<String, Object>>... rest) {

        List<List<Map<String, Object>>> seq = new ArrayList<>();
        seq.add(first);
        for (List<Map<String, Object>> r : rest) {
            seq.add(r);
        }

        int[] counter = {0};

        return new JdbcTemplate() {
            @Override
            public List<Map<String, Object>> queryForList(String sql, Object... args) {
                // 호출 횟수 초과 시 마지막 결과 반환 (Mockito willReturn 체이닝과 동일)
                int idx = Math.min(counter[0]++, seq.size() - 1);
                return seq.get(idx);
            }
        };
    }

    /** tasklet 생성 헬퍼 — JdbcTemplate stub과 함께 */
    @SafeVarargs
    private HourlyConsultTasklet makeTasklet(
            List<Map<String, Object>> first,
            List<Map<String, Object>>... rest) {

        return new HourlyConsultTasklet(
                mongoTemplate,
                analyzeService,
                makeJdbcStub(first, rest),
                "2026-03-03",
                "09-12"
        );
    }

    // ─────────────────────────────────────────────────────
    //  공통 MongoDB 집계 stub 헬퍼
    // ─────────────────────────────────────────────────────

    private void stubCategoryAggregation(List<Document> docs) {
        given(aggregationResults.getMappedResults()).willReturn(docs);
        given(mongoTemplate.aggregate(any(Aggregation.class), eq("consultation_summary"), eq(Document.class)))
                .willReturn(aggregationResults);
    }

    // ─────────────────────────────────────────────────────
    //  테스트 케이스
    // ─────────────────────────────────────────────────────

    @Test
    @DisplayName("정상: 고객 발화 2건 → Nori 분석 2회 호출 → snapshot upsert")
    void execute_normalCase_savesSnapshot() throws Exception {

        List<Map<String, Object>> fakeRows = List.of(
                Map.of("raw_text_json",
                        "[{\"speaker\":\"고객\",\"message\":\"요금이 너무 많이 나온 것 같아서요\"},{\"speaker\":\"상담사\",\"message\":\"확인해드리겠습니다\"}]",
                        "grade_code", "VIP"),
                Map.of("raw_text_json",
                        "[{\"speaker\":\"고객\",\"message\":\"기기변경 가능한지 문의드립니다\"},{\"speaker\":\"상담사\",\"message\":\"네 도와드리겠습니다\"}]",
                        "grade_code", "DIAMOND")
        );

        // 1번(슬롯)만 fakeRows, 2·3번(daily·byCustomerType)은 빈 리스트
        // → analyzeService.analyze가 정확히 2회 호출됨
        HourlyConsultTasklet tasklet = makeTasklet(fakeRows, List.of(), List.of());

        given(analyzeService.analyze(contains("요금"))).willReturn(List.of("요금", "납부"));
        given(analyzeService.analyze(contains("기기변경"))).willReturn(List.of("기기변경", "단말"));
        given(mongoTemplate.findOne(any(Query.class), eq(Document.class), anyString())).willReturn(null);

        Document categoryDoc = new Document("_id", "요금/납부")
                .append("count", 2).append("avgDurationSec", 180.0);
        stubCategoryAggregation(List.of(categoryDoc));

        // when
        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        // then
        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
        verify(mongoTemplate, times(1)).upsert(any(Query.class), any(Update.class), eq("daily_report_snapshot"));
        verify(analyzeService, times(2)).analyze(anyString());
    }

    @Test
    @DisplayName("고객 발화 없는 원문 → analyzeService 호출 안 됨")
    void execute_noCustomerSpeech_skipsAnalysis() throws Exception {

        List<Map<String, Object>> fakeRows = List.of(
                Map.of("raw_text_json",
                        "[{\"speaker\":\"상담사\",\"message\":\"안녕하세요 LG유플러스입니다\"}]",
                        "grade_code", "VIP")
        );

        // 상담사 발화만 있으므로 3번 모두 fakeRows여도 analyze 미호출
        HourlyConsultTasklet tasklet = makeTasklet(fakeRows);

        given(mongoTemplate.findOne(any(Query.class), eq(Document.class), anyString())).willReturn(null);
        stubCategoryAggregation(List.of());

        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
        verify(analyzeService, never()).analyze(anyString());
    }

    @Test
    @DisplayName("raw_text_json 파싱 실패 → 해당 건 스킵, 정상 건은 처리")
    void execute_malformedJson_skipAndContinue() throws Exception {

        List<Map<String, Object>> fakeRows = List.of(
                Map.of("raw_text_json", "INVALID_JSON", "grade_code", "VIP"),
                Map.of("raw_text_json",
                        "[{\"speaker\":\"고객\",\"message\":\"해지하고 싶어요\"}]",
                        "grade_code", "VVIP")
        );

        // 1번(슬롯)만 fakeRows → 정상 건 1건에 대해서만 analyze 호출
        // 2·3번 빈 리스트 → analyze 추가 호출 없음
        HourlyConsultTasklet tasklet = makeTasklet(fakeRows, List.of(), List.of());

        given(analyzeService.analyze(anyString())).willReturn(List.of("해지", "탈퇴"));
        given(mongoTemplate.findOne(any(Query.class), eq(Document.class), anyString())).willReturn(null);
        stubCategoryAggregation(List.of());

        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
        verify(analyzeService, times(1)).analyze(anyString()); // 정상 건 1건만
    }

    @Test
    @DisplayName("전일 snapshot 있을 때 증감율 포함 Update 저장")
    void execute_withPreviousSnapshot_calculatesChangeRate() throws Exception {

        List<Map<String, Object>> fakeRows = List.of(
                Map.of("raw_text_json",
                        "[{\"speaker\":\"고객\",\"message\":\"요금 납부 문의입니다\"}]",
                        "grade_code", "VIP")
        );

        // 3번 모두 fakeRows → 증감율 계산을 위한 analyze 다수 호출 허용
        HourlyConsultTasklet tasklet = makeTasklet(fakeRows);

        given(analyzeService.analyze(anyString())).willReturn(List.of("요금", "납부"));

        Document prevSnapshot = new Document("timeSlotTrend", List.of(
                new Document("slot", "09-12").append("keywordAnalysis",
                        new Document("topKeywords",
                                List.of(new Document("keyword", "요금").append("count", 10L))))))
                .append("keywordSummary", new Document("topKeywords", List.of()));

        // 호출 순서: 오늘 snapshot(null) → 전일 slot → 전일 daily
        given(mongoTemplate.findOne(any(Query.class), eq(Document.class), anyString()))
                .willReturn(null)
                .willReturn(prevSnapshot)
                .willReturn(prevSnapshot);

        stubCategoryAggregation(List.of());

        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
        ArgumentCaptor<Update> captor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).upsert(any(), captor.capture(), anyString());
        assertThat(captor.getValue()).isNotNull();
    }

    @Test
    @DisplayName("MySQL 원문 없을 때 빈 결과로 정상 완료")
    void execute_emptyRows_completesNormally() throws Exception {

        // 3번 모두 빈 리스트 → for-each 미실행, analyze 미호출
        HourlyConsultTasklet tasklet = makeTasklet(List.of());

        given(mongoTemplate.findOne(any(Query.class), eq(Document.class), anyString())).willReturn(null);
        stubCategoryAggregation(List.of());

        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
        verify(analyzeService, never()).analyze(anyString());
        verify(mongoTemplate, times(1)).upsert(any(), any(), eq("daily_report_snapshot"));
    }
}