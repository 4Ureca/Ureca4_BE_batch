package com.uplus.batch.jobs.summary_dummy.tasklet;

import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.jobs.summary_dummy.generator.ConsultationSummaryDummyGenerator;
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

  @Override
  public RepeatStatus execute(StepContribution contribution,
      ChunkContext chunkContext) {

    long count = Long.parseLong(
        chunkContext.getStepContext()
            .getJobParameters()
            .get("count")
            .toString()
    );

    int chunkSize = 1000;

    for (int i = 0; i < count; i += chunkSize) {

      int current = (int) Math.min(chunkSize, count - i);

      List<ConsultationSummary> list = new ArrayList<>(current);

      for (int j = 0; j < current; j++) {
        list.add(generator.generate());
      }

      mongoTemplate
          .bulkOps(BulkOperations.BulkMode.UNORDERED, ConsultationSummary.class)
          .insert(list)
          .execute();
    }

    return RepeatStatus.FINISHED;
  }
}