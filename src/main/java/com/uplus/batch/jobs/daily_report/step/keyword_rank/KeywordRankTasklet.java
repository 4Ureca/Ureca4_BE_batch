package com.uplus.batch.jobs.daily_report.step.keyword_rank;

import com.uplus.batch.common.elasticsearch.ElasticsearchAnalyzeService;
import com.uplus.batch.jobs.daily_report.dto.DailyReportSnapshot;
import com.uplus.batch.jobs.daily_report.dto.GradeSnapshot;
import com.uplus.batch.jobs.daily_report.step.keyword_rank.Consultation;
import com.uplus.batch.jobs.daily_report.step.keyword_rank.ConsultationReaderConfig;
import com.uplus.batch.jobs.daily_report.step.keyword_rank.KeywordAggregator;
import com.uplus.batch.jobs.daily_report.step.keyword_rank.KeywordCount;
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

  private final ConsultationReaderConfig readerConfig;
  private final ElasticsearchAnalyzeService analyzeService;
  private final MongoTemplate mongoTemplate;

  @Override
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

    var reader = readerConfig.dailyConsultationReader();
    reader.open(chunkContext.getStepContext().getStepExecution().getExecutionContext());

    KeywordAggregator aggregator = new KeywordAggregator();

    Consultation item;

    while ((item = reader.read()) != null) {

      String content = item.getContent();

      if (content == null || content.length() < 2) {
        continue;
      }

      List<String> keywords = analyzeService.analyze(content);

      aggregator.accumulate(keywords, item.getGradeCode());
    }

    reader.close();

    List<KeywordCount> topKeywords =
        aggregator.getTotalKeywordCount().entrySet().stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
//            .limit(10)
            .map(e -> new KeywordCount(e.getKey(), e.getValue()))
            .toList();

    List<GradeSnapshot> gradeSnapshots =
        aggregator.getByGradeCode().entrySet().stream()
            .map(entry -> {

              List<KeywordCount> list =
                  entry.getValue().entrySet().stream()
                      .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                      .map(e -> new KeywordCount(e.getKey(), e.getValue()))
                      .toList();

              return new GradeSnapshot(entry.getKey(), list);
            })
            .toList();

    DailyReportSnapshot snapshot =
        DailyReportSnapshot.builder()
            .date(LocalDate.now().minusDays(1))
            .topKeywords(topKeywords)
            .byGradeCode(gradeSnapshots)
            .build();


    // ===== 전체 키워드 TOP10 로그 =====
    log.info("===== 전체 키워드 TOP10 =====");

    snapshot.getTopKeywords()
        .stream()
        .limit(10)
        .forEach(k ->
            log.info("keyword={}, count={}", k.getKeyword(), k.getCount())
        );


// ===== 고객 등급별 TOP5 로그 =====
    snapshot.getByGradeCode()
        .forEach(grade -> {

          log.info("===== 고객등급 {} TOP5 키워드 =====", grade.getGradeCode());

          grade.getKeywords()
              .stream()
              .limit(5)
              .forEach(k ->
                  log.info("keyword={}, count={}", k.getKeyword(), k.getCount())
              );
        });

    mongoTemplate.save(snapshot);

    return RepeatStatus.FINISHED;
  }
}