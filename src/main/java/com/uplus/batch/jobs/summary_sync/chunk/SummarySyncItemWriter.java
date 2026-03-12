package com.uplus.batch.jobs.summary_sync.chunk;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.uplus.batch.domain.summary.dto.ConsultProductLogSyncRow;
import com.uplus.batch.domain.summary.dto.ConsultationResultSyncRow;
import com.uplus.batch.domain.summary.dto.CustomerReviewRow;
import com.uplus.batch.domain.summary.dto.RawTextRow;
import com.uplus.batch.domain.summary.dto.RetentionAnalysisRow;
import com.uplus.batch.domain.summary.dto.SummaryEventStatusRow;

import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.domain.summary.entity.ConsultationSummary.ResultProducts;

import com.uplus.batch.domain.summary.repository.ProductRepository;
import com.uplus.batch.domain.summary.repository.SummaryEventStatusRepository;

import com.uplus.batch.domain.summary.service.SummaryProcessingLockService;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class SummarySyncItemWriter implements ItemWriter<SummaryEventStatusRow> {

  private final MongoTemplate mongoTemplate;
  private final SummaryEventStatusRepository summaryEventStatusRepository;
  private final SummaryProcessingLockService lockService;
  private final ElasticsearchOperations elasticsearchOperations;

  private final KeywordProcessor keywordProcessor;
  private final SearchDocBuilder searchDocBuilder;
  private final ProductRepository productRepository;

  private final ObjectMapper objectMapper;

  @Override
  public void write(Chunk<? extends SummaryEventStatusRow> chunk) {

    List<Long> consultIds = chunk.getItems().stream()
        .map(SummaryEventStatusRow::consultId)
        .toList();

    log.info("SummarySyncItemWriter start. consultIds={}", consultIds);

    Map<Long, ConsultationResultSyncRow> results =
        summaryEventStatusRepository.findConsultationResultsByConsultIds(consultIds);

    Map<Long, List<ConsultProductLogSyncRow>> productLogs =
        summaryEventStatusRepository.findConsultProductLogs(consultIds);

    Map<Long, List<ConsultationSummary.RiskFlag>> riskFlags =
        summaryEventStatusRepository.findRiskFlags(consultIds);

    Map<Long, RetentionAnalysisRow> retention =
        summaryEventStatusRepository.findRetentionAnalysis(consultIds);

    Map<Long, CustomerReviewRow> reviews =
        summaryEventStatusRepository.findCustomerReviews(consultIds);

    Map<Long, RawTextRow> rawTexts =
        summaryEventStatusRepository.findRawTexts(consultIds);

    BulkOperations bulk =
        mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, ConsultationSummary.class);

    List<Long> completedIds = new ArrayList<>();
    List<Long> retryIds = new ArrayList<>();
    List<Long> failedIds = new ArrayList<>();

    List<IndexQuery> searchDocs = new ArrayList<>();
    List<IndexQuery> keywordDocs = new ArrayList<>();

    for (SummaryEventStatusRow event : chunk) {

      Long consultId = event.consultId();

      try {

        processOne(
            consultId, event,
            results, productLogs, riskFlags,
            retention, reviews, rawTexts,
            bulk, searchDocs, keywordDocs
        );

        completedIds.add(event.id());

      } catch (Exception e) {

        log.error("Summary sync failed. consultId={}, eventId={}, retryCount={}",
            consultId, event.id(), event.retryCount(), e);

        if (event.retryCount() + 1 >= 3) failedIds.add(event.id());
        else retryIds.add(event.id());
      }
    }

    log.info("Processing finished. searchDocs={}, keywordDocs={}",
        searchDocs.size(), keywordDocs.size());

    flushToStores(completedIds, retryIds, failedIds, bulk, searchDocs, keywordDocs);

    consultIds.forEach(lockService::unlock);
  }

  private void processOne(
      Long consultId,
      SummaryEventStatusRow event,
      Map<Long, ConsultationResultSyncRow> results,
      Map<Long, List<ConsultProductLogSyncRow>> productLogs,
      Map<Long, List<ConsultationSummary.RiskFlag>> riskFlags,
      Map<Long, RetentionAnalysisRow> retention,
      Map<Long, CustomerReviewRow> reviews,
      Map<Long, RawTextRow> rawTexts,
      BulkOperations bulk,
      List<IndexQuery> searchDocs,
      List<IndexQuery> keywordDocs
  ) throws Exception {

    ConsultationResultSyncRow row = results.get(consultId);

    if (row == null) {
      log.warn("consultation_results not found. consultId={}", consultId);
      throw new IllegalStateException("consultation_results not found");
    }

    RawTextRow rawText = rawTexts.get(consultId);

    if (rawText == null) {
      log.warn("rawText missing. consultId={}", consultId);
      return;
    }

    List<Map<String, Object>> messages =
        objectMapper.readValue(rawText.rawTextJson(), List.class);

    log.info("messages loaded. consultId={}, messageCount={}",
        consultId, messages.size());

    String mergedText = messages.stream()
        .map(m -> (String) m.get("text"))
        .collect(Collectors.joining(" "));

    String iamText =
        safe(row.iamIssue()) + " " +
            safe(row.iamAction()) + " " +
            safe(row.iamMemo());

    RetentionAnalysisRow retentionRow = retention.get(consultId);

    KeywordResult keywordResult =
        keywordProcessor.process(
            mergedText,
            iamText,
            retentionRow == null ? null : retentionRow.rawSummary()
        );

    log.info("keywordResult. consultId={}, matchKeywords={}, matchRate={}",
        consultId,
        keywordResult.matchKeywords(),
        keywordResult.matchRate());

    List<ResultProducts> resultProducts =
        buildResultProducts(productLogs.get(consultId));

    List<String> productCodes = extractProductCodes(resultProducts);

    List<String> productNames = productCodes.stream()
        .map(productRepository::findProductName)
        .filter(Objects::nonNull)
        .toList();

    Query query = Query.query(Criteria.where("consultId").is(consultId));

    Update update =
        buildUpdate(row, resultProducts, riskFlags.get(consultId),
            retentionRow, reviews.get(consultId), keywordResult);

    bulk.upsert(query, update);

    String allText =
        searchDocBuilder.buildAllText(row, retentionRow, productNames, keywordResult);

    IndexQuery searchDoc =
        searchDocBuilder.buildSearchDoc(
            consultId,
            row,
            retentionRow,
            riskFlags.get(consultId),
            productCodes,
            allText
        );

    IndexQuery keywordDoc =
        searchDocBuilder.buildKeywordDoc(consultId, row, messages);

    log.info("ES doc built. consultId={}, searchDoc={}, keywordDoc={}",
        consultId,
        searchDoc != null,
        keywordDoc != null);

    if (searchDoc != null) searchDocs.add(searchDoc);
    if (keywordDoc != null) keywordDocs.add(keywordDoc);
  }

  private void flushToStores(
      List<Long> completedIds,
      List<Long> retryIds,
      List<Long> failedIds,
      BulkOperations bulk,
      List<IndexQuery> searchDocs,
      List<IndexQuery> keywordDocs
  ) {

    if (!completedIds.isEmpty()) {

      log.info("Mongo bulk upsert. count={}", completedIds.size());

      bulk.execute();

      summaryEventStatusRepository.markCompletedBatch(completedIds);
    }

    if (!retryIds.isEmpty())
      summaryEventStatusRepository.markRetryBatch(retryIds);

    if (!failedIds.isEmpty())
      summaryEventStatusRepository.markFailedBatch(failedIds);

    log.info("Indexing to ES. searchDocs={}, keywordDocs={}",
        searchDocs.size(), keywordDocs.size());

    if (!searchDocs.isEmpty() || !keywordDocs.isEmpty()) {
      indexToElasticsearch(completedIds, searchDocs, keywordDocs);
    }
  }

  private void indexToElasticsearch(
      List<Long> consultIds,
      List<IndexQuery> searchDocs,
      List<IndexQuery> keywordDocs
  ) {

    try {

      if (!searchDocs.isEmpty()) {
        log.info("ES bulkIndex searchDocs size={}", searchDocs.size());
        elasticsearchOperations.bulkIndex(searchDocs, IndexCoordinates.of("consult-search-index"));
        markIndexed(consultIds, "searchIndexed", "searchIndexedAt");
      }

      if (!keywordDocs.isEmpty()) {
        log.info("ES bulkIndex keywordDocs size={}", keywordDocs.size());
        elasticsearchOperations.bulkIndex(keywordDocs, IndexCoordinates.of("consult-keyword-index"));
        markIndexed(consultIds, "keywordIndexed", "keywordIndexedAt");
      }

    } catch (Exception e) {
      log.error("ES indexing failed. searchIndexed/keywordIndexed=false 유지. consultIds={}", consultIds, e);
    }
  }

  private void markIndexed(List<Long> consultIds, String indexedField, String indexedAtField) {
    BulkOperations bulk =
        mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, ConsultationSummary.class);
    LocalDateTime now = LocalDateTime.now();
    for (Long consultId : consultIds) {
      bulk.updateOne(
          Query.query(Criteria.where("consultId").is(consultId)),
          new Update().set(indexedField, true).set(indexedAtField, now)
      );
    }
    bulk.execute();
    log.info("MongoDB {} 업데이트 완료. count={}", indexedField, consultIds.size());
  }

  private String safe(String v) {
    return v == null ? "" : v;
  }

  private List<String> extractProductCodes(List<ResultProducts> resultProducts) {
    if (resultProducts == null) return List.of();

    List<String> codes = new ArrayList<>();

    for (ResultProducts rp : resultProducts) {
      if (rp.getSubscribed() != null) codes.addAll(rp.getSubscribed());
      if (rp.getCanceled() != null) codes.addAll(rp.getCanceled());
    }

    return codes;
  }

  private List<ResultProducts> buildResultProducts(List<ConsultProductLogSyncRow> logs) {

    if (logs == null || logs.isEmpty()) return null;

    List<String> subscribed = new ArrayList<>();
    List<String> canceled = new ArrayList<>();
    List<ResultProducts.Conversion> conversion = new ArrayList<>();
    List<String> recommitment = new ArrayList<>();

    for (ConsultProductLogSyncRow log : logs) {

      String newProduct = extractNewProduct(log);
      String canceledProduct = extractCanceledProduct(log);

      switch (log.contractType()) {

        case "NEW" -> {
          if (newProduct != null) subscribed.add(newProduct);
        }

        case "CANCEL" -> {
          if (canceledProduct != null) canceled.add(canceledProduct);
        }

        case "CHANGE" -> conversion.add(
            ResultProducts.Conversion.builder()
                .subscribed(newProduct)
                .canceled(canceledProduct)
                .build()
        );

        case "RENEW" -> {
          if (newProduct != null) recommitment.add(newProduct);
        }
      }
    }

    List<ResultProducts> results = new ArrayList<>();

    if (!subscribed.isEmpty())
      results.add(ResultProducts.builder()
          .subscribed(subscribed)
          .changeType("NEW")
          .build());

    if (!canceled.isEmpty())
      results.add(ResultProducts.builder()
          .canceled(canceled)
          .changeType("CANCEL")
          .build());

    if (!conversion.isEmpty())
      results.add(ResultProducts.builder()
          .conversion(conversion)
          .changeType("CHANGE")
          .build());

    if (!recommitment.isEmpty())
      results.add(ResultProducts.builder()
          .recommitment(recommitment)
          .changeType("RENEW")
          .build());

    return results;
  }

  private String extractNewProduct(ConsultProductLogSyncRow log) {

    if (log.newProductHome() != null) return log.newProductHome();
    if (log.newProductMobile() != null) return log.newProductMobile();
    if (log.newProductService() != null) return log.newProductService();

    return null;
  }

  private String extractCanceledProduct(ConsultProductLogSyncRow log) {

    if (log.canceledProductHome() != null) return log.canceledProductHome();
    if (log.canceledProductMobile() != null) return log.canceledProductMobile();
    if (log.canceledProductService() != null) return log.canceledProductService();

    return null;
  }

  private Double calculateScore(CustomerReviewRow r) {

    if (r == null) return null;

    return (r.score1() + r.score2() + r.score3() + r.score4() + r.score5()) / 5.0;
  }

  private Update buildUpdate(
      ConsultationResultSyncRow row,
      List<ResultProducts> resultProducts,
      List<ConsultationSummary.RiskFlag> riskFlags,
      RetentionAnalysisRow retention,
      CustomerReviewRow review,
      KeywordResult keywordResult
  ) {

    return new Update()
        .set("consultId", row.consultId())
        .set("consultedAt", row.createdAt())
        .set("channel", row.channel())
        .set("durationSec", row.durationSec())

        .set("iam",
            ConsultationSummary.Iam.builder()
                .issue(row.iamIssue())
                .action(row.iamAction())
                .memo(row.iamMemo())
                .matchKeyword(keywordResult.matchKeywords())
                .matchRates(keywordResult.matchRate())
                .build()
        )

        .set("agent",
            ConsultationSummary.Agent.builder()
                .id(row.employeeId())
                .name(row.employeeName())
                .build()
        )

        .set("customer",
            ConsultationSummary.Customer.builder()
                .id(row.customerId())
                .type(row.customerType())
                .phone(row.customerPhone())
                .name(row.customerName())
                .ageGroup(row.ageGroup())
                .grade(row.customerGrade())
                .gender(row.customerGender())
                .satisfiedScore(calculateScore(review))
                .build()
        )

        .set("category",
            ConsultationSummary.Category.builder()
                .code(row.categoryCode())
                .large(row.categoryLarge())
                .medium(row.categoryMedium())
                .small(row.categorySmall())
                .build()
        )

        .set("riskFlags", riskFlags)

        .set("summary",
            retention == null ? null :
                ConsultationSummary.Summary.builder()
                    .content(retention.rawSummary())
                    .keywords(keywordResult.summaryKeywords())
                    .build()
        )

        .set("cancellation",
            retention == null ? null :
                ConsultationSummary.Cancellation.builder()
                    .intent(retention.hasIntent())
                    .defenseAttempted(retention.defenseAttempted())
                    .defenseSuccess(retention.defenseSuccess())
                    .defenseActions(retention.defenseActions())
                    .complaintReasons(retention.complaintReason())
                    .build()
        )

        .set("resultProducts", resultProducts)

        .setOnInsert("createdAt", LocalDateTime.now());
  }
}