package com.uplus.batch.jobs.daily_report.step.quality_analysis;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
//import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
@StepScope
@Slf4j
public class QualityAnalysisTasklet implements Tasklet {

  private final MongoTemplate mongoTemplate;
  private final String startDateParam;
  private final String targetCollection; // "daily_report_snapshot" 등

  public QualityAnalysisTasklet(
      MongoTemplate mongoTemplate,
      @Value("#{jobParameters['startDate'] ?: jobParameters['date']}") String startDateParam, // date도 허용!
      @Value("#{jobParameters['targetCollection'] ?: 'weekly_report_snapshot'}") String targetCollection) {
    this.mongoTemplate = mongoTemplate;
    this.startDateParam = startDateParam;
    this.targetCollection = targetCollection;
  }

  @Override
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
    // 1. targetCollection에서 agent 컬렉션 명칭 추론 (PerformanceTasklet과 동일 로직)
    String agentCollection = targetCollection.replace("_report_snapshot", "_agent_report_snapshot");

    // 2. 날짜 처리
    LocalDate startDate = LocalDate.parse(startDateParam);
    LocalDateTime startAt = startDate.atStartOfDay();

    log.info("[QualityAnalysis] {} 기반으로 {} 분석 시작", targetCollection, agentCollection);

    // 3. 상담사 데이터 조회 및 평균 계산
    Query query = new Query(Criteria.where("startAt").is(startAt));
    List<Document> agentSnaps = mongoTemplate.find(query, Document.class, agentCollection);

    if (agentSnaps.isEmpty()) return RepeatStatus.FINISHED;

    double avgScore = agentSnaps.stream()
        .map(doc -> (Document) doc.get("qualityAnalysis"))
        .filter(Objects::nonNull)
        .mapToDouble(qa -> {
          Object score = qa.get("totalScore");
          return score instanceof Number ? ((Number) score).doubleValue() : 0.0;
        })
        .average().orElse(0.0);

    // 4. 결과 업데이트
    Update update = new Update().set("performanceSummary.avgQualityScore", Math.round(avgScore * 100.0) / 100.0);
    mongoTemplate.updateFirst(query, update, targetCollection);

    return RepeatStatus.FINISHED;
  }
}