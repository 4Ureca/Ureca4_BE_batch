package com.uplus.batch.jobs.daily_report.step.customer_risk;

import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.*;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("CustomerRiskTasklet")
class CustomerRiskTaskletTest {

    @Mock MongoTemplate mongoTemplate;
    @Mock JdbcTemplate jdbcTemplate;

    private CustomerRiskTasklet tasklet;
    private StepContribution contribution;
    private ChunkContext chunkContext;

    @BeforeEach
    void setUp() {
        tasklet = new CustomerRiskTasklet(mongoTemplate, jdbcTemplate, "2025-01-15");
        JobExecution jobExecution = new JobExecution(1L, new JobParameters());
        StepExecution stepExecution = new StepExecution("customerRiskStep", jobExecution);
        contribution = new StepContribution(stepExecution);
        chunkContext = new ChunkContext(new StepContext(stepExecution));
    }

    /** CHURN 포함 7종 Mock */
    private void mockActiveRiskTypes() {
        doAnswer(invocation -> {
            RowCallbackHandler handler = invocation.getArgument(1);
            var rs = mock(java.sql.ResultSet.class);
            String[][] types = {
                {"ABUSE", "폭언/욕설"}, {"FRAUD", "사기 의심"}, {"POLICY", "정책 악용"},
                {"COMP", "과도한 보상 요구"}, {"REPEAT", "반복 민원"},
                {"CHURN", "해지 위험"}, {"PHISHING", "피싱 피해"}
            };
            for (String[] t : types) {
                when(rs.getString("type_code")).thenReturn(t[0]);
                when(rs.getString("type_name")).thenReturn(t[1]);
                handler.processRow(rs);
            }
            return null;
        }).when(jdbcTemplate).query(anyString(), any(RowCallbackHandler.class));
    }

    @Nested
    @DisplayName("정의서 필드 매핑")
    class FieldMapping {

        @Test
        @DisplayName("COMP 1건 + CHURN 1건 -> excessiveCompensation=1, churnRisk=1")
        void givenFlags_whenExecute_thenMappedCorrectly() throws Exception {
            mockActiveRiskTypes();
            AggregationResults<Document> aggResults = new AggregationResults<>(List.of(
                    new Document("_id", "COMP").append("count", 1),
                    new Document("_id", "CHURN").append("count", 1)
            ), new Document());
            when(mongoTemplate.aggregate(any(Aggregation.class), eq("consultation_summary"), eq(Document.class)))
                    .thenReturn(aggResults);

            tasklet.execute(contribution, chunkContext);

            CustomerRiskResult result = (CustomerRiskResult) contribution.getStepExecution()
                    .getJobExecution().getExecutionContext()
                    .get(CustomerRiskTasklet.RESULT_KEY);

            assertThat(result).isNotNull();
            assertThat(result.getExcessiveCompensation()).isEqualTo(1);
            assertThat(result.getChurnRisk()).isEqualTo(1);
            assertThat(result.getFraudSuspect()).isZero();
            assertThat(result.getMaliciousComplaint()).isZero();
            assertThat(result.getPolicyAbuse()).isZero();
            assertThat(result.getRepeatedComplaint()).isZero();
            assertThat(result.getPhishingVictim()).isZero();
            assertThat(result.getTotalRiskCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("FRAUD 3 + ABUSE 5 + REPEAT 2 -> 정의서 필드별 정확히 매핑")
        void givenMultipleFlags_whenExecute_thenAllMapped() throws Exception {
            mockActiveRiskTypes();
            AggregationResults<Document> aggResults = new AggregationResults<>(List.of(
                    new Document("_id", "FRAUD").append("count", 3),
                    new Document("_id", "ABUSE").append("count", 5),
                    new Document("_id", "REPEAT").append("count", 2)
            ), new Document());
            when(mongoTemplate.aggregate(any(Aggregation.class), eq("consultation_summary"), eq(Document.class)))
                    .thenReturn(aggResults);

            tasklet.execute(contribution, chunkContext);

            CustomerRiskResult result = (CustomerRiskResult) contribution.getStepExecution()
                    .getJobExecution().getExecutionContext()
                    .get(CustomerRiskTasklet.RESULT_KEY);

            assertThat(result.getFraudSuspect()).isEqualTo(3);
            assertThat(result.getMaliciousComplaint()).isEqualTo(5);
            assertThat(result.getRepeatedComplaint()).isEqualTo(2);
            assertThat(result.getChurnRisk()).isZero();
            assertThat(result.getTotalRiskCount()).isEqualTo(10);
        }
    }

    @Nested
    @DisplayName("빈 데이터")
    class EmptyData {
        @Test
        @DisplayName("riskFlags 없는 날 -> 전체 0건")
        void givenNoRiskFlags_whenExecute_thenAllZero() throws Exception {
            mockActiveRiskTypes();
            AggregationResults<Document> emptyResults = new AggregationResults<>(List.of(), new Document());
            when(mongoTemplate.aggregate(any(Aggregation.class), eq("consultation_summary"), eq(Document.class)))
                    .thenReturn(emptyResults);

            tasklet.execute(contribution, chunkContext);

            CustomerRiskResult result = (CustomerRiskResult) contribution.getStepExecution()
                    .getJobExecution().getExecutionContext()
                    .get(CustomerRiskTasklet.RESULT_KEY);

            assertThat(result.getTotalRiskCount()).isZero();
            assertThat(result.getChurnRisk()).isZero();
        }
    }

}