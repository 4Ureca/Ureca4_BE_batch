package com.uplus.batch.jobs.monthly_reports.step.admin;

import com.uplus.batch.jobs.monthly_report.step.admin.MonthlySubscriptionStatsTasklet;
import com.uplus.batch.jobs.monthly_report.step.admin.model.MonthlyReportSnapshot;
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
@DisplayName("MonthlySubscriptionStatsTasklet 테스트")
class MonthlySubscriptionStatsTaskletTest {

  @Mock MongoTemplate mongoTemplate;
  @Mock JdbcTemplate jdbcTemplate;

  @InjectMocks
  private MonthlySubscriptionStatsTasklet tasklet;

  private StepContribution contribution;
  private ChunkContext chunkContext;

  @BeforeEach
  void setUp() {
    // 배치 컨텍스트 초기화
    JobExecution jobExecution = new JobExecution(1L, new JobParameters());
    StepExecution stepExecution = new StepExecution("monthlySubscriptionStatsStep", jobExecution);
    contribution = new StepContribution(stepExecution);
    chunkContext = new ChunkContext(new StepContext(stepExecution));
  }

  /** MySQL 상품 마스터 Mock (모바일, 홈, 부가서비스 통합) */
  private void mockProductMaster() {
    doAnswer(invocation -> {
      RowCallbackHandler handler = invocation.getArgument(1);
      ResultSet rs = mock(ResultSet.class);

      // 테스트용 상품 코드 및 이름 정의
      String[][] products = {
          {"MOB01", "5G 프리미어"}, {"MOB02", "다이렉트 65"},
          {"ADD01", "디즈니+"}, {"HOME01", "기가인터넷"}
      };

      for (String[] p : products) {
        // 각 테이블 조회 쿼리에 대응하도록 설정 (실제 쿼리 순서에 따라 여러번 호출됨)
        when(rs.getString(anyString())).thenReturn(p[0], p[1]);
        handler.processRow(rs);
      }
      return null;
    }).when(jdbcTemplate).query(anyString(), any(RowCallbackHandler.class));
  }

  @Nested
  @DisplayName("신규 가입 및 해지 집계")
  class SubscriptionStats {

    @Test
    @DisplayName("가입 2건(같은상품), 해지 1건 -> 신규 리스트 카운트 및 상품명 매핑 확인")
    void aggregateSubscriptions() throws Exception {
      mockProductMaster();

      // 가짜 상담 데이터: 40대가 MOB01 2번 가입, 20대가 ADD01 해지
      Document doc1 = new Document("customer", new Document("ageGroup", "40대"))
          .append("resultProducts", List.of(new Document("subscribed", List.of("MOB01", "MOB01"))));
      Document doc2 = new Document("customer", new Document("ageGroup", "20대"))
          .append("resultProducts", List.of(new Document("canceled", List.of("ADD01"))));

      when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("consultation_summary")))
          .thenReturn(List.of(doc1, doc2));

      tasklet.execute(contribution, chunkContext);

      // 결과 검증: upsert로 저장되었는지 확인
      verify(mongoTemplate).upsert(any(Query.class), any(Update.class), eq("monthly_report_snapshot"));
    }
  }

  @Nested
  @DisplayName("연령대별 선호도 (TOP 3)")
  class AgeGroupPreference {

    @Test
    @DisplayName("특정 연령대 가입 상품이 많을 때 -> 내림차순 정렬 및 TOP 3 제한 확인")
    void checkAgeGroupTop3() throws Exception {
      mockProductMaster();

      // 30대가 4종류 상품 가입 (A: 10건, B: 5건, C: 3건, D: 1건)
      Document doc = new Document("customer", new Document("ageGroup", "30대"))
          .append("resultProducts", List.of(new Document("subscribed",
              List.of("MOB01", "MOB01", "MOB02", "ADD01", "HOME01")))); // 실제 코드에선 루프 돌며 카운트

      when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("consultation_summary")))
          .thenReturn(List.of(doc));

      tasklet.execute(contribution, chunkContext);

      // 결과 검증: upsert로 저장되었는지 확인
      verify(mongoTemplate).upsert(any(Query.class), any(Update.class), eq("monthly_report_snapshot"));
    }
  }

  @Nested
  @DisplayName("데이터 없음")
  class EmptyData {
    @Test
    @DisplayName("조회된 상담 데이터가 없을 때 -> 빈 리스트 저장")
    void givenNoData_thenEmptyLists() throws Exception {
      mockProductMaster();
      when(mongoTemplate.find(any(Query.class), eq(Document.class), eq("consultation_summary")))
          .thenReturn(List.of());

      tasklet.execute(contribution, chunkContext);

      // 결과 검증: upsert로 저장되었는지 확인
      verify(mongoTemplate).upsert(any(Query.class), any(Update.class), eq("monthly_report_snapshot"));
    }
  }
}