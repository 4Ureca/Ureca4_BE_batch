package com.uplus.batch.jobs.summary_sync.chunk;

import com.uplus.batch.domain.summary.dto.ConsultProductLogSyncRow;
import com.uplus.batch.domain.summary.dto.ConsultationResultSyncRow;
import com.uplus.batch.domain.summary.dto.CustomerReviewRow;
import com.uplus.batch.domain.summary.dto.RawTextRow;
import com.uplus.batch.domain.summary.dto.RetentionAnalysisRow;
import com.uplus.batch.domain.summary.dto.SummaryEventStatusRow;
import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.domain.summary.entity.ConsultationSummary.ResultProducts;
import com.uplus.batch.domain.summary.repository.SummaryEventStatusRepository;
import com.uplus.batch.domain.summary.service.SummaryProcessingLockService;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SummarySyncItemWriter implements ItemWriter<SummaryEventStatusRow> {

  private final MongoTemplate mongoTemplate;
  private final SummaryEventStatusRepository summaryEventStatusRepository;
  private final SummaryProcessingLockService lockService;

  @Override
  public void write(Chunk<? extends SummaryEventStatusRow> chunk) {

    BulkOperations bulk =
        mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, ConsultationSummary.class);

    List<Long> consultIds = new ArrayList<>();
    List<Long> eventIds = new ArrayList<>();

    for (SummaryEventStatusRow item : chunk) {
      consultIds.add(item.consultId());
      eventIds.add(item.id());
    }

    Map<Long, ConsultationResultSyncRow> results =
        summaryEventStatusRepository.findConsultationResultsByConsultIds(consultIds);
    Map<Long, List<ConsultProductLogSyncRow>> productLogs =
        summaryEventStatusRepository.findConsultProductLogs(consultIds);
    Map<Long, List<String>> riskFlags =
        summaryEventStatusRepository.findRiskFlags(consultIds);
    Map<Long, RetentionAnalysisRow> retention =
        summaryEventStatusRepository.findRetentionAnalysis(consultIds);
    Map<Long, CustomerReviewRow> reviews =
        summaryEventStatusRepository.findCustomerReviews(consultIds);
    Map<Long, RawTextRow> rawTexts =
        summaryEventStatusRepository.findRawTexts(consultIds);

    List<Long> completedIds = new ArrayList<>();
    List<Long> retryIds = new ArrayList<>();
    List<Long> failedIds = new ArrayList<>();

    for (SummaryEventStatusRow event : chunk) {

      Long consultId = event.consultId();

      try {

        ConsultationResultSyncRow row = results.get(consultId);

        if (row == null) {
          throw new IllegalStateException("consultation_results not found");
        }

        List<ResultProducts> resultProducts =
            buildResultProducts(productLogs.get(consultId));

        Query query = Query.query(Criteria.where("consultId").is(consultId));
        Update update = buildUpdate(
            row,
            resultProducts,
            riskFlags.get(consultId),
            retention.get(consultId),
            reviews.get(consultId)
        );

        bulk.upsert(query, update);

        completedIds.add(event.id());

      } catch (Exception e) {

        int nextRetryCount = event.retryCount() + 1;

        if (nextRetryCount >= 3) {
          failedIds.add(event.id());
        } else {
          retryIds.add(event.id());
        }
      }
    }

    bulk.execute();

    if (!completedIds.isEmpty()) {
      summaryEventStatusRepository.markCompletedBatch(completedIds);
    }

    if (!retryIds.isEmpty()) {
      summaryEventStatusRepository.markRetryBatch(retryIds);
    }

    if (!failedIds.isEmpty()) {
      summaryEventStatusRepository.markFailedBatch(failedIds);
    }

    for (Long consultId : consultIds) {
      lockService.unlock(consultId);
    }
  }

  private Update buildUpdate(
      ConsultationResultSyncRow row,
      List<ResultProducts> resultProducts,
      List<String> riskFlags,
      RetentionAnalysisRow retention,
      CustomerReviewRow review
  ) {

    return new Update()
        .set("consultId", row.consultId())
        .set("consultedAt", row.createdAt())
        .set("channel", row.channel())
        .set("durationSec", row.durationSec())
        .set(
            "iam",
            ConsultationSummary.Iam.builder()
                .issue(row.iamIssue())
                .action(row.iamAction())
                .memo(row.iamMemo())
                .build())
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

  private List<ResultProducts> buildResultProducts(List<ConsultProductLogSyncRow> logs) {

    if (logs == null || logs.isEmpty()) {
      return null;
    }

    List<String> subscribed = new ArrayList<>();
    List<String> canceled = new ArrayList<>();
    List<ResultProducts.Conversion> conversion = new ArrayList<>();
    List<String> recommitment = new ArrayList<>();

    for (ConsultProductLogSyncRow log : logs) {

      String newProduct = extractNewProduct(log);
      String canceledProduct = extractCanceledProduct(log);

      switch (log.contractType()) {

        case "NEW" -> {
          if (newProduct != null) {
            subscribed.add(newProduct);
          }
        }

        case "CANCEL" -> {
          if (canceledProduct != null) {
            canceled.add(canceledProduct);
          }
        }

        case "CHANGE" -> {
          conversion.add(
              ResultProducts.Conversion.builder()
                  .subscribed(newProduct)
                  .canceled(canceledProduct)
                  .build()
          );
        }

        case "RENEW" -> {
          if (newProduct != null) {
            recommitment.add(newProduct);
          }
        }
      }
    }

    List<ResultProducts> results = new ArrayList<>();

    if (!subscribed.isEmpty()) {
      results.add(
          ResultProducts.builder()
              .subscribed(subscribed)
              .changeType("NEW")
              .build()
      );
    }

    if (!canceled.isEmpty()) {
      results.add(
          ResultProducts.builder()
              .canceled(canceled)
              .changeType("CANCEL")
              .build()
      );
    }

    if (!conversion.isEmpty()) {
      results.add(
          ResultProducts.builder()
              .conversion(conversion)
              .changeType("CHANGE")
              .build()
      );
    }

    if (!recommitment.isEmpty()) {
      results.add(
          ResultProducts.builder()
              .recommitment(recommitment)
              .changeType("RENEW")
              .build()
      );
    }

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

    return (
        r.score1()
            + r.score2()
            + r.score3()
            + r.score4()
            + r.score5()
    ) / 5.0;
  }
}