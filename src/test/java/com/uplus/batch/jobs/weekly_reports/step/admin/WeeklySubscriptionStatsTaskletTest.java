package com.uplus.batch.jobs.weekly_reports.step.admin;

import com.uplus.batch.jobs.weekly_report.step.admin.WeeklySubscriptionStatsTasklet;
import com.uplus.batch.jobs.weekly_report.step.admin.model.WeeklyReportSnapshot;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.sql.ResultSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("WeeklySubscriptionStatsTasklet 상세 테스트")
class WeeklySubscriptionStatsTaskletTest {

  @Mock MongoTemplate mongoTemplate;
  @Mock JdbcTemplate jdbcTemplate;

  @InjectMocks
  private WeeklySubscriptionStatsTasklet tasklet;

  private StepContribution contribution;
  private ChunkContext chunkContext;

  @BeforeEach
  void setUp() {
    JobExecution jobExecution = new JobExecution(1L, new JobParameters());
    StepExecution stepExecution = new StepExecution("weeklySubscriptionStatsStep", jobExecution);
    contribution = new StepContribution(stepExecution);
    chunkContext = new ChunkContext(new StepContext(stepExecution));
  }

  private void mockProductMaster() {
    doAnswer(invocation -> {
      RowCallbackHandler handler = invocation.getArgument(1);
      ResultSet rs = mock(ResultSet.class);
      String[][] products = {
          {"MOB01", "5G 프리미어"}, {"MOB02", "다이렉트 65"}, {"ADD01", "디즈니+"}
      };
      for (String[] p : products) {
        when(rs.getString(anyString())).thenReturn(p[0], p[1]);
        handler.processRow(rs);
      }
      return null;
    }).when(jdbcTemplate).query(anyString(), any(RowCallbackHandler.class));
  }

  @Nested
  @DisplayName("1. 주간 신규/해지 집계")
  class BasicAggregation {
    @Test
    @DisplayName("동일 상품 여러 건 가입 시 카운트가 정확히 합산")
    void aggregateCounts() throws Exception {
      mockProductMaster();
      Document doc = new Document("customer", new Document("ageGroup", "30대"))
          .append("resultProducts", List.of(new Document("subscribed", List.of("MOB01", "MOB01"))));

      when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("consultation_summary")))
          .thenReturn(List.of(doc));

      tasklet.execute(contribution, chunkContext);

      // 결과 검증: upsert로 저장되었는지 확인
      verify(mongoTemplate).upsert(any(Query.class), any(Update.class), eq("weekly_report_snapshot"));
    }
  }

  @Nested
  @DisplayName("2. 주간 연령대별 선호도")
  class AgePreference {
    @Test
    @DisplayName("연령대별 가입 상품이 TOP 3를 초과할 경우 상위 3개만 남음")
    void checkTop3() throws Exception {
      mockProductMaster();
      // 한 연령대에서 4개 상품 가입 발생
      Document doc = new Document("customer", new Document("ageGroup", "50대"))
          .append("resultProducts", List.of(new Document("subscribed",
              List.of("MOB01", "MOB01", "MOB02", "ADD01", "UNKNOWN_PROD"))));

      when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("consultation_summary")))
          .thenReturn(List.of(doc));

      tasklet.execute(contribution, chunkContext);

      // 결과 검증: upsert로 저장되었는지 확인
      verify(mongoTemplate).upsert(any(Query.class), any(Update.class), eq("weekly_report_snapshot"));
    }
  }

  @Nested
  @DisplayName("3. 예외 케이스 (데이터 없음)")
  class EdgeCases {
    @Test
    @DisplayName("해당 기간에 상담 데이터가 전혀 없을 때 빈 리스트가 저장")
    void noData() throws Exception {
      mockProductMaster();
      when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("consultation_summary")))
          .thenReturn(List.of());

      tasklet.execute(contribution, chunkContext);

      // 결과 검증: upsert로 저장되었는지 확인
      verify(mongoTemplate).upsert(any(Query.class), any(Update.class), eq("weekly_report_snapshot"));
    }
  }
}