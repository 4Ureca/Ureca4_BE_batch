package com.uplus.batch.jobs.summary_sync.chunk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.summary.dto.*;
import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.domain.summary.entity.ConsultationSummary.ResultProducts;
import com.uplus.batch.domain.summary.repository.ProductRepository;
import com.uplus.batch.domain.summary.repository.SummaryEventStatusRepository;
import com.uplus.batch.domain.summary.service.SummaryProcessingLockService;
import com.uplus.batch.domain.summary.service.builder.SearchDocBuilder;
import com.uplus.batch.jobs.summary_sync.chunk.KeywordProcessor.KeywordResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.*;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.*;
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
    List<IndexQuery> searchDocs = new ArrayList<>();
    List<IndexQuery> keywordDocs = new ArrayList<>();

    for (SummaryEventStatusRow event : chunk) {

      Long consultId = event.consultId();

      try {

        ConsultationResultSyncRow row = results.get(consultId);
        RawTextRow rawText = rawTexts.get(consultId);

        if (row == null || rawText == null) continue;

        List<Map<String, Object>> messages =
            objectMapper.readValue(rawText.rawTextJson(), List.class);

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

        List<ResultProducts> resultProducts =
            buildResultProducts(productLogs.get(consultId));

        List<String> productCodes = extractProductCodes(resultProducts);

        List<String> productNames = productCodes.stream()
            .map(productRepository::findProductName)
            .filter(Objects::nonNull)
            .toList();

        ConsultationSummary summary =
            buildSummaryObject(row, resultProducts, riskFlags.get(consultId),
                retentionRow, reviews.get(consultId), keywordResult);

        Query query = Query.query(Criteria.where("consultId").is(consultId));
        bulk.upsert(query, buildUpdateFromSummary(summary));

        searchDocs.add(
            searchDocBuilder.buildSearchDoc(summary, productCodes, productNames, keywordResult)
        );

        keywordDocs.add(
            searchDocBuilder.buildKeywordDoc(summary, messages)
        );

        completedIds.add(event.id());

      } catch (Exception e) {

        log.error("summary sync failed consultId={}", consultId, e);
      }
    }

    if (!completedIds.isEmpty()) {
      bulk.execute();
      summaryEventStatusRepository.markCompletedBatch(completedIds);
    }

    indexToElasticsearch(consultIds, searchDocs, keywordDocs);

    consultIds.forEach(lockService::unlock);
  }

  private void indexToElasticsearch(
      List<Long> consultIds,
      List<IndexQuery> searchDocs,
      List<IndexQuery> keywordDocs
  ) {

    if (!searchDocs.isEmpty()) {
      elasticsearchOperations.bulkIndex(searchDocs,
          IndexCoordinates.of("consult-search-index"));
      markIndexed(consultIds, "searchIndexed", "searchIndexedAt");
    }

    if (!keywordDocs.isEmpty()) {
      elasticsearchOperations.bulkIndex(keywordDocs,
          IndexCoordinates.of("consult-keyword-index"));
      markIndexed(consultIds, "keywordIndexed", "keywordIndexedAt");
    }
  }

  private void markIndexed(List<Long> consultIds, String field, String fieldAt) {

    BulkOperations bulk =
        mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, ConsultationSummary.class);

    LocalDateTime now = LocalDateTime.now();

    for (Long consultId : consultIds) {

      bulk.updateOne(
          Query.query(Criteria.where("consultId").is(consultId)),
          new Update().set(field, true).set(fieldAt, now)
      );
    }

    bulk.execute();
  }

  private Update buildUpdateFromSummary(ConsultationSummary s) {

    Update update = new Update()
        .set("consultId", s.getConsultId())
        .set("consultedAt", s.getConsultedAt())
        .set("channel", s.getChannel())
        .set("durationSec", s.getDurationSec())
        .set("iam", s.getIam())
        .set("agent", s.getAgent())
        .set("customer", s.getCustomer())
        .set("category", s.getCategory())
        .set("riskFlags", s.getRiskFlags())
        .set("summary", s.getSummary())
        .set("resultProducts", s.getResultProducts())
        .set("cancellation", s.getCancellation())
        .set("outbound", s.getOutbound());

    update.setOnInsert("createdAt", LocalDateTime.now());

    return update;
  }

  private ConsultationSummary buildSummaryObject(
      ConsultationResultSyncRow row,
      List<ResultProducts> resultProducts,
      List<ConsultationSummary.RiskFlag> riskFlags,
      RetentionAnalysisRow retention,
      CustomerReviewRow review,
      KeywordResult keywordResult
  ) {

    return ConsultationSummary.builder()
        .consultId(row.consultId())
        .consultedAt(row.createdAt())
        .channel(row.channel())
        .durationSec(row.durationSec())
        .resultProducts(resultProducts)
        .riskFlags(riskFlags)

        .iam(
            ConsultationSummary.Iam.builder()
                .issue(row.iamIssue())
                .action(row.iamAction())
                .memo(row.iamMemo())
                .matchKeyword(keywordResult.matchKeywords())
                .matchRates(keywordResult.matchRate())
                .build()
        )

        .agent(
            ConsultationSummary.Agent.builder()
                .id(row.employeeId())
                .name(row.employeeName())
                .build()
        )

        .customer(
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

        .category(
            ConsultationSummary.Category.builder()
                .code(row.categoryCode())
                .large(row.categoryLarge())
                .medium(row.categoryMedium())
                .small(row.categorySmall())
                .build()
        )

        .summary(
            retention == null ? null :
                ConsultationSummary.Summary.builder()
                    .content(retention.rawSummary())
                    .keywords(keywordResult.summaryKeywords())
                    .build()
        )

        .cancellation(
            retention == null ? null :
                ConsultationSummary.Cancellation.builder()
                    .intent(retention.hasIntent())
                    .defenseAttempted(retention.defenseAttempted())
                    .defenseSuccess(retention.defenseSuccess())
                    .defenseActions(retention.defenseActions())
                    .complaintReasons(retention.complaintReason())
                    .complaintCategory(retention.complaintCategory())
                    .defenseCategory(retention.defenseCategory())
                    .build()
        )

        .outbound(
            retention == null ? null :
                ConsultationSummary.Outbound.builder()
                    .callResult(retention.outboundCallResult())
                    .rejectReason(retention.outboundCategory())
                    .outboundReport(retention.outboundReport())
                    .build()
        )

        .build();
  }

  private String safe(String v) {
    return v == null ? "" : v;
  }

  private Double calculateScore(CustomerReviewRow r) {
    if (r == null) return null;
    return (r.score1() + r.score2() + r.score3() + r.score4() + r.score5()) / 5.0;
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

    for (ConsultProductLogSyncRow log : logs) {

      String newProduct = extractNewProduct(log);
      String canceledProduct = extractCanceledProduct(log);

      if ("NEW".equals(log.contractType()) && newProduct != null)
        subscribed.add(newProduct);

      if ("CANCEL".equals(log.contractType()) && canceledProduct != null)
        canceled.add(canceledProduct);
    }

    List<ResultProducts> results = new ArrayList<>();

    if (!subscribed.isEmpty())
      results.add(ResultProducts.builder().subscribed(subscribed).changeType("NEW").build());

    if (!canceled.isEmpty())
      results.add(ResultProducts.builder().canceled(canceled).changeType("CANCEL").build());

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
}