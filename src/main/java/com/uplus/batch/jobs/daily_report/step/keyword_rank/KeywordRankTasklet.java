package com.uplus.batch.jobs.daily_report.step.keyword_rank;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import com.uplus.batch.common.elasticsearch.ElasticsearchAnalyzeService;
import com.uplus.batch.jobs.daily_report.dto.DailyReportSnapshot;
import com.uplus.batch.jobs.daily_report.dto.GradeSnapshot;
import com.uplus.batch.jobs.daily_report.step.keyword_rank.Consultation;
import com.uplus.batch.jobs.daily_report.step.keyword_rank.ConsultationReaderConfig;
import com.uplus.batch.jobs.daily_report.step.keyword_rank.KeywordAggregator;
import com.uplus.batch.jobs.daily_report.step.keyword_rank.KeywordCount;
import java.time.LocalTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class KeywordRankTasklet implements Tasklet {

  private final MongoTemplate mongoTemplate;

  private final ElasticsearchClient elasticsearchClient; // 직접 주입


  @Override
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
      throws Exception {

     // [실제 운영용]
//    LocalDate targetDate = LocalDate.now().minusDays(1);
    //[테스트용]
    LocalDate targetDate = LocalDate.of(2025, 1, 15);

    String startOfDay = targetDate.atStartOfDay().toString();
    String endOfDay = targetDate.atTime(LocalTime.MAX).toString();

    // 1. 쿼리 객체 생성 (고객 발화 키워드 집계용)
    SearchResponse<Void> response = elasticsearchClient.search(s -> s
            .index("consult-keyword-index")
            .size(0)
            .query(q -> q
                .range(r -> r
                    .date(d -> d
                        .field("date")
                        .gte(startOfDay)
                        .lte(endOfDay)
                    )
                )
            )
            .aggregations("total_keywords", a -> a
                .terms(t -> t.field("customer.search").size(20))
            )
            .aggregations("by_grade", a -> a
                .terms(t -> t.field("customer_grade"))
                .aggregations("grade_keywords", subA -> subA
                    .terms(t -> t.field("customer.search").size(6))
                )
            ),
        Void.class
    );

    // 2. 결과 파싱 (필드명과 일치하도록 수정)
    List<KeywordCount> topKeywords = response.aggregations().get("total_keywords").sterms()
        .buckets().array().stream()
        .map(b -> new KeywordCount(b.key().stringValue(), b.docCount()))
        // "null" 이라는 문자열을 가진 키워드 제외, 한글자 제외
        .filter(k -> !"null".equals(k.getKeyword()) && k.getKeyword().length() > 1)
        .toList();

    List<GradeSnapshot> gradeSnapshots = response.aggregations().get("by_grade").sterms().buckets()
        .array().stream()
        .map(gradeBucket -> {
          List<KeywordCount> keywords = gradeBucket.aggregations().get("grade_keywords")
              .sterms().buckets().array().stream()
              .map(k -> new KeywordCount(k.key().stringValue(), k.docCount()))
              // "null" 이라는 문자열을 가진 키워드 제외, 한글자 제외
              .filter(k -> !"null".equals(k.getKeyword()) && k.getKeyword().length() > 1)
              .toList();
          return new GradeSnapshot(gradeBucket.key().stringValue(), keywords);
        })
        .toList();

    // 3. Snapshot 생성 및 저장
    DailyReportSnapshot snapshot = DailyReportSnapshot.builder()
        .date(targetDate)
        .topKeywords(topKeywords)
        .byGradeCode(gradeSnapshots)
        .build();

    mongoTemplate.save(snapshot);

    return RepeatStatus.FINISHED;
  }
}