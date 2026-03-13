package com.uplus.batch.jobs.es_reindex.chunk;

import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.domain.summary.service.SummaryProcessingLockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Iterator;
import java.util.List;

@Slf4j
@StepScope
@RequiredArgsConstructor
public class EsReindexItemReader implements ItemReader<ConsultationSummary> {

  private final MongoTemplate mongoTemplate;
  private final SummaryProcessingLockService lockService;

  private Iterator<ConsultationSummary> iterator;

  @Override
  public ConsultationSummary read() {

    if (iterator == null) {
      init();
    }

    while (iterator.hasNext()) {
      ConsultationSummary summary = iterator.next();

      // 락 획득 실패 = 이미 다른 스레드가 처리 중 → 스킵
      if (!lockService.tryReindexLock(summary.getConsultId())) {
        log.debug("재인덱싱 처리중 스킵. consultId={}", summary.getConsultId());
        continue;
      }

      return summary;
    }

    return null;
  }

  private void init() {
    Criteria searchNotDone = new Criteria().orOperator(
        Criteria.where("searchIndexed").exists(false),
        Criteria.where("searchIndexed").is(false)
    );
    Criteria keywordNotDone = new Criteria().orOperator(
        Criteria.where("keywordIndexed").exists(false),
        Criteria.where("keywordIndexed").is(false)
    );

    Query query = Query.query(
        new Criteria().orOperator(searchNotDone, keywordNotDone)
    );

    List<ConsultationSummary> list = mongoTemplate.find(query, ConsultationSummary.class);
    log.info("ES 재처리 대상: {}건", list.size());
    iterator = list.iterator();
  }
}