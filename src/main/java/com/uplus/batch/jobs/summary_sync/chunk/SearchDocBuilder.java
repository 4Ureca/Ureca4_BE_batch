package com.uplus.batch.jobs.summary_sync.chunk;

import com.uplus.batch.domain.summary.dto.*;
import com.uplus.batch.domain.summary.entity.ConsultationSummary.ResultProducts;
import com.uplus.batch.jobs.summary_sync.chunk.KeywordProcessor.KeywordResult;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.stereotype.Component;

import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
public class SearchDocBuilder {

  private static final DateTimeFormatter ES_DATE =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

  /**
   * 전문 검색용 인덱스 문서 생성 (consult-search-index)
   */
  public IndexQuery buildSearchDoc(
      Long consultId,
      ConsultationResultSyncRow row,
      RetentionAnalysisRow retention,
      List<String> riskFlags,
      List<String> productCodes,
      KeywordResult keywordResult,
      String allText
  ) {
    Map<String, Object> doc = new HashMap<>();

    doc.put("consultId", consultId);
    doc.put("allText", allText);
    doc.put("summaryContent", retention == null ? null : retention.rawSummary());

    doc.put("customerName", row.customerName());
    doc.put("phone", row.customerPhone());
    doc.put("customerId", row.customerId());
    doc.put("ageGroup", row.ageGroup());
    doc.put("grade", row.customerGrade());
    doc.put("gender", row.customerGender());

    doc.put("agentId", row.employeeId());
    doc.put("agentName", row.employeeName());

    doc.put("categoryCode", row.categoryCode());
    doc.put("categoryLarge", row.categoryLarge());
    doc.put("categoryMedium", row.categoryMedium());
    doc.put("categorySmall", row.categorySmall());

    doc.put("riskFlags", riskFlags);
    doc.put("intent", retention == null ? null : retention.hasIntent());
    doc.put("defenseAttempted", retention == null ? null : retention.defenseAttempted());
    doc.put("defenseSuccess", retention == null ? null : retention.defenseSuccess());
    doc.put("products", productCodes);
    doc.put("durationSec", row.durationSec());
    doc.put("consultedAt", row.createdAt().format(ES_DATE));

    IndexQuery query = new IndexQuery();
    query.setId(String.valueOf(consultId));
    query.setObject(doc);
    return query;
  }

  /**
   * 키워드 분석용 인덱스 문서 생성 (consult-keyword-index)
   * content 필드에 search_analyzer / quality_analyzer 두 개가 매핑되어
   * ES 인덱싱 시점에 각각 분석됨
   */
  public IndexQuery buildKeywordDoc(
      Long consultId,
      ConsultationResultSyncRow row,
      String mergedText
  ) {
    Map<String, Object> doc = new HashMap<>();

    doc.put("content", mergedText);
    doc.put("agent_id", row.employeeId());
    doc.put("customer_grade", row.customerGrade());
    doc.put("date", row.createdAt().format(ES_DATE));

    IndexQuery query = new IndexQuery();
    query.setId(String.valueOf(consultId));
    query.setObject(doc);
    return query;
  }

  /**
   * allText 조합: summary + iam 필드 + 상품명 + 키워드
   */
  public String buildAllText(
      ConsultationResultSyncRow row,
      RetentionAnalysisRow retention,
      List<String> productNames,
      KeywordResult keywordResult
  ) {
    List<String> parts = new ArrayList<>();

    if (retention != null && retention.rawSummary() != null)
      parts.add(retention.rawSummary());

    parts.add(safe(row.iamIssue()));
    parts.add(safe(row.iamAction()));
    parts.add(safe(row.iamMemo()));
    parts.addAll(productNames);
    parts.addAll(keywordResult.matchKeywords());

    if (keywordResult.summaryKeywords() != null)
      parts.addAll(keywordResult.summaryKeywords());

    return parts.stream()
        .filter(s -> !s.isBlank())
        .collect(java.util.stream.Collectors.joining(" "));
  }

  private String safe(String v) {
    return v == null ? "" : v;
  }
}