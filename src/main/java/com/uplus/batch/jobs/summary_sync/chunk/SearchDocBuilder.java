package com.uplus.batch.jobs.summary_sync.chunk;

import com.uplus.batch.domain.summary.dto.*;
import com.uplus.batch.jobs.summary_sync.chunk.KeywordProcessor.KeywordResult;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.stereotype.Component;

import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
public class SearchDocBuilder {

  private static final DateTimeFormatter ES_DATE =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

  // -----------------------------------------------------------------------
  // 기존 - ConsultationResultSyncRow 기반 (SummarySyncItemWriter에서 사용)
  // -----------------------------------------------------------------------

  public IndexQuery buildSearchDoc(
      Long consultId,
      ConsultationResultSyncRow row,
      RetentionAnalysisRow retention,
      List<String> riskFlags,
      List<String> productCodes,
      String allText
  ) {
    Map<String, Object> doc = new HashMap<>();

    doc.put("consultId", consultId);
    doc.put("allText", allText);
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

  public IndexQuery buildKeywordDoc(
      Long consultId,
      ConsultationResultSyncRow row,
      List<Map<String, Object>> messages
  ) {
    String agentText = messages.stream()
        .filter(m -> "상담사".equals(m.get("speaker")))
        .map(m -> (String) m.get("text"))
        .collect(java.util.stream.Collectors.joining(" "));

    String customerText = messages.stream()
        .filter(m -> "고객".equals(m.get("speaker")))
        .map(m -> (String) m.get("text"))
        .collect(java.util.stream.Collectors.joining(" "));

    Map<String, Object> doc = new HashMap<>();
    doc.put("agent", agentText);
    doc.put("customer", customerText);
    doc.put("agent_id", row.employeeId());
    doc.put("customer_grade", row.customerGrade());
    doc.put("date", row.createdAt().format(ES_DATE));

    IndexQuery query = new IndexQuery();
    query.setId(String.valueOf(consultId));
    query.setObject(doc);
    return query;
  }

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

  // -----------------------------------------------------------------------
  // ES 재처리용 오버로드 - ConsultationSummary 엔티티 기반 (EsReindexItemWriter에서 사용)
  // -----------------------------------------------------------------------

  public IndexQuery buildSearchDoc(
      com.uplus.batch.domain.summary.entity.ConsultationSummary summary,
      List<String> productCodes,
      KeywordResult keywordResult
  ) {
    com.uplus.batch.domain.summary.entity.ConsultationSummary.Customer c = summary.getCustomer();
    com.uplus.batch.domain.summary.entity.ConsultationSummary.Agent a = summary.getAgent();
    com.uplus.batch.domain.summary.entity.ConsultationSummary.Category cat = summary.getCategory();
    com.uplus.batch.domain.summary.entity.ConsultationSummary.Cancellation can = summary.getCancellation();

    List<String> parts = new ArrayList<>();
    if (summary.getSummary() != null && summary.getSummary().getContent() != null)
      parts.add(summary.getSummary().getContent());
    if (summary.getIam() != null) {
      parts.add(safe(summary.getIam().getIssue()));
      parts.add(safe(summary.getIam().getAction()));
      parts.add(safe(summary.getIam().getMemo()));
    }
    parts.addAll(keywordResult.matchKeywords());
    if (keywordResult.summaryKeywords() != null)
      parts.addAll(keywordResult.summaryKeywords());

    String allText = parts.stream()
        .filter(s -> !s.isBlank())
        .collect(java.util.stream.Collectors.joining(" "));

    Map<String, Object> doc = new HashMap<>();
    doc.put("consultId", summary.getConsultId());
    doc.put("allText", allText);
    doc.put("customerName", c == null ? null : c.getName());
    doc.put("phone", c == null ? null : c.getPhone());
    doc.put("customerId", c == null ? null : c.getId());
    doc.put("ageGroup", c == null ? null : c.getAgeGroup());
    doc.put("grade", c == null ? null : c.getGrade());
    doc.put("gender", c == null ? null : c.getGender());
    doc.put("agentId", a == null ? null : a.getId());
    doc.put("agentName", a == null ? null : a.getName());
    doc.put("categoryCode", cat == null ? null : cat.getCode());
    doc.put("categoryLarge", cat == null ? null : cat.getLarge());
    doc.put("categoryMedium", cat == null ? null : cat.getMedium());
    doc.put("categorySmall", cat == null ? null : cat.getSmall());
    doc.put("riskFlags", summary.getRiskFlags());
    doc.put("intent", can == null ? null : can.getIntent());
    doc.put("defenseAttempted", can == null ? null : can.getDefenseAttempted());
    doc.put("defenseSuccess", can == null ? null : can.getDefenseSuccess());
    doc.put("products", productCodes);
    doc.put("durationSec", summary.getDurationSec());
    doc.put("consultedAt",
        summary.getConsultedAt() == null ? null : summary.getConsultedAt().format(ES_DATE));

    IndexQuery query = new IndexQuery();
    query.setId(String.valueOf(summary.getConsultId()));
    query.setObject(doc);
    return query;
  }

  public IndexQuery buildKeywordDoc(
      Long consultId,
      List<Map<String, Object>> messages,
      com.uplus.batch.domain.summary.entity.ConsultationSummary summary
  ) {
    String agentText = messages.stream()
        .filter(m -> "상담사".equals(m.get("speaker")))
        .map(m -> (String) m.get("text"))
        .collect(java.util.stream.Collectors.joining(" "));

    String customerText = messages.stream()
        .filter(m -> "고객".equals(m.get("speaker")))
        .map(m -> (String) m.get("text"))
        .collect(java.util.stream.Collectors.joining(" "));

    Map<String, Object> doc = new HashMap<>();
    doc.put("agent", agentText);
    doc.put("customer", customerText);
    doc.put("agent_id", summary.getAgent() == null ? null : summary.getAgent().getId());
    doc.put("customer_grade", summary.getCustomer() == null ? null : summary.getCustomer().getGrade());
    doc.put("date", summary.getConsultedAt() == null ? null : summary.getConsultedAt().format(ES_DATE));

    IndexQuery query = new IndexQuery();
    query.setId(String.valueOf(consultId));
    query.setObject(doc);
    return query;
  }

  private String safe(String v) {
    return v == null ? "" : v;
  }
}