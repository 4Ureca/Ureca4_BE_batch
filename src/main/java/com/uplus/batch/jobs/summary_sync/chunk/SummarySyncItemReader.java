package com.uplus.batch.jobs.summary_sync.chunk;

import com.uplus.batch.domain.summary.dto.SummaryEventStatusRow;
import com.uplus.batch.domain.summary.repository.SummaryEventStatusRepository;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/*
summary_event_status READ
 - 상태가 REQUESTED
 - job 실행 시간 이전에 생성된 이벤트 대상
*/
@Component
@StepScope
@RequiredArgsConstructor
public class SummarySyncItemReader implements ItemReader<SummaryEventStatusRow> {

  private final SummaryEventStatusRepository summaryEventStatusRepository;

  @Value("${app.summary-sync.batch-size:500}")
  private int batchSize;

  @Value("#{jobParameters['runAt']}")
  private Long runAt;

  private long lastEventId = 0L;
  private List<SummaryEventStatusRow> currentBatch = new ArrayList<>();
  private int currentIndex = 0;

  @Override
  public SummaryEventStatusRow read() {

    if (currentIndex >= currentBatch.size()) {

      LocalDateTime jobStartTime = LocalDateTime.ofInstant(
          Instant.ofEpochMilli(runAt),
          ZoneId.systemDefault()
      );

      currentBatch = summaryEventStatusRepository.findRequestedBatchAfterIdBeforeCreatedAt(
          lastEventId,
          batchSize,
          jobStartTime
      );

      currentIndex = 0;

      if (currentBatch.isEmpty()) {
        return null;
      }
    }

    SummaryEventStatusRow item = currentBatch.get(currentIndex++);
    lastEventId = item.id();

    return item;
  }
}