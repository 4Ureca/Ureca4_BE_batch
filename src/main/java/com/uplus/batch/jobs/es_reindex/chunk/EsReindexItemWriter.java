package com.uplus.batch.jobs.es_reindex.chunk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.summary.dto.RawTextRow;
import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.domain.summary.repository.SummaryEventStatusRepository;
import com.uplus.batch.jobs.summary_sync.chunk.KeywordProcessor;
import com.uplus.batch.jobs.summary_sync.chunk.KeywordProcessor.KeywordResult;
import com.uplus.batch.jobs.summary_sync.chunk.SearchDocBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class EsReindexItemWriter implements ItemWriter<ConsultationSummary> {

  private final ElasticsearchOperations elasticsearchOperations;
  private final MongoTemplate mongoTemplate;
  private final SummaryEventStatusRepository summaryEventStatusRepository;
  private final KeywordProcessor keywordProcessor;
  private final SearchDocBuilder searchDocBuilder;
  private final ObjectMapper objectMapper;

  @Override
  public void write(Chunk<? extends ConsultationSummary> chunk) {

    List<Long> consultIds = chunk.getItems().stream()
        .map(ConsultationSummary::getConsultId)
        .toList();

    // MySQL에서 rawText 일괄 조회 (기존 SummarySyncItemWriter 방식 동일)
    Map<Long, RawTextRow> rawTexts =
        summaryEventStatusRepository.findRawTexts(consultIds);

    List<IndexQuery> searchDocs = new ArrayList<>();
    List<IndexQuery> keywordDocs = new ArrayList<>();
    List<Long> successIds = new ArrayList<>();

    for (ConsultationSummary summary : chunk) {
      Long consultId = summary.getConsultId();
      try {
        RawTextRow rawText = rawTexts.get(consultId);
        if (rawText == null) {
          log.warn("rawText 없음. 스킵. consultId={}", consultId);
          continue;
        }

        // rawText → mergedText (기존과 동일)
        List<Map<String, Object>> messages =
            objectMapper.readValue(rawText.rawTextJson(), List.class);
        String mergedText = messages.stream()
            .map(m -> (String) m.get("text"))
            .collect(Collectors.joining(" "));

        // iam 텍스트 조합
        String iamText =
            safe(summary.getIam() == null ? null : summary.getIam().getIssue()) + " " +
                safe(summary.getIam() == null ? null : summary.getIam().getAction()) + " " +
                safe(summary.getIam() == null ? null : summary.getIam().getMemo());

        String rawSummary =
            summary.getSummary() == null ? null : summary.getSummary().getContent();

        // 키워드 분석 (기존과 동일)
        KeywordResult keywordResult = keywordProcessor.process(mergedText, iamText, rawSummary);

        // 상품 코드 추출
        List<String> productCodes = extractProductCodes(summary.getResultProducts());

        searchDocs.add(searchDocBuilder.buildSearchDoc(summary, productCodes, keywordResult));
        keywordDocs.add(searchDocBuilder.buildKeywordDoc(consultId, messages, summary));

        successIds.add(consultId);

      } catch (Exception e) {
        log.error("ES 재처리 실패. consultId={}", consultId, e);
      }
    }

    if (successIds.isEmpty()) return;

    try {
      if (!searchDocs.isEmpty())
        elasticsearchOperations.bulkIndex(searchDocs, IndexCoordinates.of("consult-search-index"));

      if (!keywordDocs.isEmpty())
        elasticsearchOperations.bulkIndex(keywordDocs, IndexCoordinates.of("consult-keyword-index"));

      markIndexed(successIds);
      log.info("ES 재처리 완료: {}건", successIds.size());

    } catch (Exception e) {
      log.error("ES 재처리 bulkIndex 실패. searchIndexed=false 유지. consultIds={}", successIds, e);
    }
  }

  private void markIndexed(List<Long> consultIds) {
    BulkOperations bulk =
        mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, ConsultationSummary.class);

    for (Long consultId : consultIds) {
      bulk.updateOne(
          Query.query(Criteria.where("consultId").is(consultId)),
          new Update()
              .set("searchIndexed", true)
              .set("searchIndexedAt", LocalDateTime.now())
      );
    }

    bulk.execute();
  }

  private List<String> extractProductCodes(List<ConsultationSummary.ResultProducts> resultProducts) {
    if (resultProducts == null) return List.of();
    List<String> codes = new ArrayList<>();
    for (ConsultationSummary.ResultProducts rp : resultProducts) {
      if (rp.getSubscribed() != null) codes.addAll(rp.getSubscribed());
      if (rp.getCanceled() != null) codes.addAll(rp.getCanceled());
      if (rp.getConversion() != null)
        rp.getConversion().forEach(c -> {
          if (c.getSubscribed() != null) codes.add(c.getSubscribed());
          if (c.getCanceled() != null) codes.add(c.getCanceled());
        });
      if (rp.getRecommitment() != null) codes.addAll(rp.getRecommitment());
    }
    return codes;
  }

  private String safe(String v) {
    return v == null ? "" : v;
  }
}