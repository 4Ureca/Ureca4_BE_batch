package com.uplus.batch.jobs.summary_sync.chunk;

import com.uplus.batch.domain.summary.dto.SummaryEventStatusRow;
import com.uplus.batch.domain.summary.service.SummaryProcessingLockService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SummarySyncItemProcessor
    implements ItemProcessor<SummaryEventStatusRow, SummaryEventStatusRow> {

  private final SummaryProcessingLockService lockService;

  @Override
  public SummaryEventStatusRow process(SummaryEventStatusRow item) {

    if (!lockService.tryLock(item.consultId())) {
      return null;
    }

    return item;
  }
}
