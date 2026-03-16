package com.uplus.batch.domain.summary.service.builder;

import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.jobs.summary_sync.chunk.KeywordProcessor.KeywordResult;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.stereotype.Component;

@Component
public class SearchDocBuilder {

  private static final DateTimeFormatter ES_DATE =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

  public IndexQuery buildSearchDoc(
      ConsultationSummary summary,
      List<String> productCodes,
      List<String> productNames,
      KeywordResult keywordResult
  ) {

    String allText = buildAllText(summary, productNames, keywordResult);

    ConsultationSummary.Customer c = summary.getCustomer();
    ConsultationSummary.Agent a = summary.getAgent();
    ConsultationSummary.Category cat = summary.getCategory();
    ConsultationSummary.Cancellation can = summary.getCancellation();
    ConsultationSummary.Outbound out = summary.getOutbound();

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

    doc.put("channel", summary.getChannel());

    List<Map<String, String>> riskFlagDocs =
        summary.getRiskFlags() == null ? List.of() :
            summary.getRiskFlags().stream()
                .map(r -> Map.of("riskType", r.getRiskType(), "riskLevel", r.getRiskLevel()))
                .toList();

    doc.put("riskFlags", riskFlagDocs);

    doc.put("intent", can == null ? null : can.getIntent());
    doc.put("defenseAttempted", can == null ? null : can.getDefenseAttempted());
    doc.put("defenseSuccess", can == null ? null : can.getDefenseSuccess());
    doc.put("complaintCategory", can == null ? null : can.getComplaintCategory());
    doc.put("defenseCategory", can == null ? null : can.getDefenseCategory());
    doc.put("outboundResult", out == null ? null : out.getCallResult());
    doc.put("outboundCategory", out == null ? null : out.getRejectReason());

    doc.put("products", productCodes);
    doc.put("durationSec", summary.getDurationSec());

    doc.put("consultedAt",
        summary.getConsultedAt() == null ? null :
            summary.getConsultedAt().format(ES_DATE));

    IndexQuery query = new IndexQuery();
    query.setId(String.valueOf(summary.getConsultId()));
    query.setObject(doc);

    return query;
  }

  public IndexQuery buildKeywordDoc(
      ConsultationSummary summary,
      List<Map<String, Object>> messages
  ) {

    String agentText = messages.stream()
        .filter(m -> "상담사".equals(m.get("speaker")))
        .map(m -> (String) m.get("text"))
        .collect(Collectors.joining(" "));

    String customerText = messages.stream()
        .filter(m -> "고객".equals(m.get("speaker")))
        .map(m -> (String) m.get("text"))
        .collect(Collectors.joining(" "));

    Map<String, Object> doc = new HashMap<>();

    doc.put("agent", agentText);
    doc.put("customer", customerText);
    doc.put("agent_id", summary.getAgent() == null ? null : summary.getAgent().getId());
    doc.put("customer_grade", summary.getCustomer() == null ? null : summary.getCustomer().getGrade());

    doc.put("date",
        summary.getConsultedAt() == null ? null :
            summary.getConsultedAt().format(ES_DATE));

    IndexQuery query = new IndexQuery();
    query.setId(String.valueOf(summary.getConsultId()));
    query.setObject(doc);

    return query;
  }

  public String buildAllText(
      ConsultationSummary summary,
      List<String> productNames,
      KeywordResult keywordResult
  ) {

    List<String> parts = new ArrayList<>();

    if (summary.getSummary() != null && summary.getSummary().getContent() != null)
      parts.add(summary.getSummary().getContent());

    if (summary.getIam() != null) {
      parts.add(safe(summary.getIam().getIssue()));
      parts.add(safe(summary.getIam().getAction()));
      parts.add(safe(summary.getIam().getMemo()));
    }

    parts.addAll(productNames);
    parts.addAll(keywordResult.matchKeywords());

    if (keywordResult.summaryKeywords() != null)
      parts.addAll(keywordResult.summaryKeywords());

    return parts.stream()
        .filter(s -> !s.isBlank())
        .collect(Collectors.joining(" "));
  }

  private String safe(String v) {
    return v == null ? "" : v;
  }
}