package com.uplus.batch.jobs.summary_dummy.tasklet;

import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.jobs.summary_dummy.dto.ConsultationResultRow;
import com.uplus.batch.jobs.summary_dummy.generator.ConsultationSummaryDummyGenerator;
import com.uplus.batch.jobs.summary_dummy.repository.ConsultationResultRepository;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ConsultationSummaryDummyTasklet implements Tasklet {

  private final MongoTemplate mongoTemplate;
  private final ConsultationSummaryDummyGenerator generator;
  private final ConsultationResultRepository resultRepository;

  @Override
  public RepeatStatus execute(StepContribution contribution,
      ChunkContext chunkContext) {

    long startId = Long.parseLong(
        chunkContext.getStepContext()
            .getJobParameters()
            .get("startId")
            .toString()
    );

    long endId = Long.parseLong(
        chunkContext.getStepContext()
            .getJobParameters()
            .get("endId")
            .toString()
    );

    if (startId > endId) {
      throw new IllegalArgumentException("startId must be <= endId");
    }

    int chunkSize = 100;
    long currentStart = startId;

    while (currentStart <= endId) {

      long currentEnd = Math.min(currentStart + chunkSize - 1, endId);

      List<ConsultationResultRow> rows =
          resultRepository.findByRange(currentStart, currentEnd);

      if (!rows.isEmpty()) {

        List<ConsultationSummary> summaries =
            new ArrayList<>(rows.size());

        for (ConsultationResultRow row : rows) {
          summaries.add(generator.generate(row));
        }

        mongoTemplate
            .bulkOps(BulkOperations.BulkMode.UNORDERED, ConsultationSummary.class)
            .insert(summaries)
            .execute();
      }

      currentStart = currentEnd + 1;
    }

    return RepeatStatus.FINISHED;
  }
}