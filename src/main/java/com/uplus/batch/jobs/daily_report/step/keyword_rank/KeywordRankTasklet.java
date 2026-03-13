package com.uplus.batch.jobs.daily_report.step.keyword_rank;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.uplus.batch.jobs.daily_report.dto.DailyReportSnapshot;
import com.uplus.batch.jobs.daily_report.dto.GradeSnapshot;
import com.uplus.batch.jobs.daily_report.step.keyword_rank.KeywordCount;
import java.time.LocalTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
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

    var jobParams = chunkContext.getStepContext().getJobParameters();
    String targetDateStr = (String) jobParams.get("targetDate");
    LocalDate targetDate = targetDateStr != null
            ? LocalDate.parse(targetDateStr)
            : LocalDate.now().minusDays(1);

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

    // 2. 결과 파싱 (null 키워드, 한글자 키워드 제외)
    List<KeywordCount> topKeywords = response.aggregations().get("total_keywords").sterms()
        .buckets().array().stream()
        .map(b -> new KeywordCount(b.key().stringValue(), b.docCount()))
        .filter(k -> !"null".equals(k.getKeyword()) && k.getKeyword().length() > 1)
        .toList();

    List<GradeSnapshot> gradeSnapshots = response.aggregations().get("by_grade").sterms().buckets()
        .array().stream()
        .map(gradeBucket -> {
          List<KeywordCount> keywords = gradeBucket.aggregations().get("grade_keywords")
              .sterms().buckets().array().stream()
              .map(k -> new KeywordCount(k.key().stringValue(), k.docCount()))
              .filter(k -> !"null".equals(k.getKeyword()) && k.getKeyword().length() > 1)
              .toList();
          return new GradeSnapshot(gradeBucket.key().stringValue(), keywords);
        })
        .toList();

    // Document로 변환하여 _class 메타데이터 저장 방지
    List<Document> topKeywordDocs = topKeywords.stream()
        .map(k -> new Document("keyword", k.getKeyword()).append("count", k.getCount()))
        .toList();

    List<Document> gradeDocs = gradeSnapshots.stream()
        .map(g -> {
          List<Document> kwDocs = g.getKeywords().stream()
              .map(k -> new Document("keyword", k.getKeyword()).append("count", k.getCount()))
              .toList();
          return new Document("customerType", g.getGradeCode()).append("keywords", kwDocs);
        })
        .toList();

    Query upsertQuery = new Query();
    upsertQuery.addCriteria(Criteria.where("startAt").is(targetDate));

    Update update = new Update()
        .set("keywordSummary.topKeywords", topKeywordDocs)
        .set("keywordSummary.byCustomerType", gradeDocs);

    mongoTemplate.upsert(upsertQuery, update, DailyReportSnapshot.class);

    return RepeatStatus.FINISHED;
  }
}