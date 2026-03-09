package com.uplus.batch.jobs.es_reindex.chunk;

import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.domain.summary.service.SummaryProcessingLockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class EsReindexItemReader implements ItemReader<ConsultationSummary> {

  private final MongoTemplate mongoTemplate;
  private final SummaryProcessingLockService lockService;

  private Iterator<ConsultationSummary> iterator;
  private boolean initialized = false;

  @Override
  public ConsultationSummary read() {

    if (!initialized) {
      init();
    }

    while (iterator.hasNext()) {
      ConsultationSummary summary = iterator.next();

      // 레디스 락 걸려있으면 스킵
      if (lockService.tryLock(summary.getConsultId())) {
        log.debug("요약 데이터 처리중 스킵. consultId={}", summary.getConsultId());
        continue;
      }

      return summary;
    }

    return null;
  }

  private void init() {
    Query query = Query.query(
        new Criteria().orOperator(
            Criteria.where("searchIndexed").exists(false),
            Criteria.where("searchIndexed").is(false)
        )
    );

    List<ConsultationSummary> list = mongoTemplate.find(query, ConsultationSummary.class);
    log.info("ES 재처리 대상: {}건", list.size());
    iterator = list.iterator();
    initialized = true;
  }
}