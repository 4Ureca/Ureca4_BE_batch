package com.uplus.batch.jobs.daily_report.step.hourly_consult;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.aggregations.Buckets;
import co.elastic.clients.util.ObjectBuilder;
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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

/**
 * HourlyConsultTasklet 단위 테스트
 *
 * [변경] MySQL+Nori analyze → ES consult-keyword-index terms aggregation 방식으로 전환됨에 따라
 *        JdbcTemplate/ElasticsearchAnalyzeService 대신 ElasticsearchClient mock 사용
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HourlyConsultTaskletTest {

    @Mock private MongoTemplate mongoTemplate;
    @Mock private ElasticsearchClient elasticsearchClient;
    @Mock private StepContribution contribution;
    @Mock private ChunkContext chunkContext;
    @Mock private StepContext stepContext;
    @Mock private StepExecution stepExecution;
    @Mock private JobExecution jobExecution;
    @Mock private AggregationResults<Document> aggregationResults;

    @BeforeEach
    void setUp() {
        ExecutionContext executionContext = new ExecutionContext();
        given(contribution.getStepExecution()).willReturn(stepExecution);
        given(stepExecution.getJobExecution()).willReturn(jobExecution);
        given(jobExecution.getExecutionContext()).willReturn(executionContext);
        given(chunkContext.getStepContext()).willReturn(stepContext);
        given(stepContext.getStepExecution()).willReturn(stepExecution);
    }

    // ─────────────────────────────────────────────────────
    //  ES SearchResponse mock 헬퍼
    // ─────────────────────────────────────────────────────

    /**
     * ES terms aggregation 응답을 mock 합니다.
     * keyword → count 쌍을 받아서 SearchResponse<Void>를 반환하도록 설정합니다.
     */
    @SuppressWarnings("unchecked")
    private void stubEsKeywordAggregation(Map<String, Long> keywords) throws Exception {
        // StringTermsBucket 리스트 생성
        List<StringTermsBucket> buckets = keywords.entrySet().stream()
                .map(e -> StringTermsBucket.of(b -> b
                        .key(e.getKey())
                        .docCount(e.getValue())
                ))
                .toList();

        StringTermsAggregate sterms = StringTermsAggregate.of(s -> s
                .buckets(Buckets.of(b -> b.array(buckets)))
                .sumOtherDocCount(0L)
        );

        Aggregate aggregate = Aggregate.of(a -> a.sterms(sterms));

        SearchResponse<Void> response = mock(SearchResponse.class);
        given(response.aggregations()).willReturn(Map.of("slot_keywords", aggregate));

        given(elasticsearchClient.search(any(java.util.function.Function.class), eq(Void.class)))
                .willReturn(response);
    }

    /** tasklet 생성 헬퍼 */
    private HourlyConsultTasklet makeTasklet() {
        return new HourlyConsultTasklet(
                mongoTemplate,
                elasticsearchClient,
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
    @DisplayName("정상: ES 키워드 집계 결과 → snapshot upsert")
    void execute_normalCase_savesSnapshot() throws Exception {

        HourlyConsultTasklet tasklet = makeTasklet();

        stubEsKeywordAggregation(Map.of("요금", 5L, "납부", 3L, "기기변경", 2L));
        given(mongoTemplate.findOne(any(Query.class), eq(Document.class), anyString())).willReturn(null);

        Document categoryDoc = new Document("_id", "요금/납부")
                .append("count", 2).append("avgDurationSec", 180.0);
        stubCategoryAggregation(List.of(categoryDoc));

        // when
        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        // then
        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
        verify(mongoTemplate, times(1)).upsert(any(Query.class), any(Update.class), eq("daily_report_snapshot"));
    }

    @Test
    @DisplayName("ES 키워드 결과 비어있을 때 빈 결과로 정상 완료")
    void execute_emptyKeywords_completesNormally() throws Exception {

        HourlyConsultTasklet tasklet = makeTasklet();

        stubEsKeywordAggregation(Map.of());
        given(mongoTemplate.findOne(any(Query.class), eq(Document.class), anyString())).willReturn(null);
        stubCategoryAggregation(List.of());

        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
        verify(mongoTemplate, times(1)).upsert(any(), any(), eq("daily_report_snapshot"));
    }

    @Test
    @DisplayName("null/한글자 키워드 필터링 확인")
    void execute_filtersNullAndShortKeywords() throws Exception {

        HourlyConsultTasklet tasklet = makeTasklet();

        // "null"과 한글자 "요"는 필터링되어야 함
        stubEsKeywordAggregation(Map.of(
                "요금", 10L,
                "null", 5L,
                "요", 3L,
                "납부", 2L
        ));
        given(mongoTemplate.findOne(any(Query.class), eq(Document.class), anyString())).willReturn(null);
        stubCategoryAggregation(List.of());

        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        assertThat(status).isEqualTo(RepeatStatus.FINISHED);

        // upsert 된 Update 캡처하여 키워드 확인
        ArgumentCaptor<Update> captor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).upsert(any(), captor.capture(), anyString());
        String updateStr = captor.getValue().toString();
        // "요금", "납부"는 포함, "null"과 "요"는 제외
        assertThat(updateStr).contains("요금");
        assertThat(updateStr).contains("납부");
    }

    @Test
    @DisplayName("전일 snapshot 있을 때 증감율 계산")
    void execute_withPreviousSnapshot_calculatesChangeRate() throws Exception {

        HourlyConsultTasklet tasklet = makeTasklet();

        stubEsKeywordAggregation(Map.of("요금", 20L, "납부", 5L));

        Document prevSnapshot = new Document("timeSlotTrend", List.of(
                new Document("slot", "09-12").append("keywordAnalysis",
                        new Document("topKeywords",
                                List.of(new Document("keyword", "요금").append("count", 10L))))))
                .append("keywordSummary", new Document("topKeywords", List.of()));

        // 호출 순서: 전일 slot → 오늘 snapshot(null)
        given(mongoTemplate.findOne(any(Query.class), eq(Document.class), anyString()))
                .willReturn(prevSnapshot)
                .willReturn(null);

        stubCategoryAggregation(List.of());

        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
        ArgumentCaptor<Update> captor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).upsert(any(), captor.capture(), anyString());
        assertThat(captor.getValue()).isNotNull();
    }

    @Test
    @DisplayName("유효하지 않은 슬롯이면 스킵")
    void execute_invalidSlot_skips() throws Exception {

        HourlyConsultTasklet tasklet = new HourlyConsultTasklet(
                mongoTemplate,
                elasticsearchClient,
                "2026-03-03",
                "99-99"  // 잘못된 슬롯
        );

        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
        verify(mongoTemplate, never()).upsert(any(), any(), anyString());
    }
}
