package com.uplus.batch.jobs.daily_report.step.keyword_rank;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KeywordAggregator {

  private final Map<String, Long> totalKeywordCount = new HashMap<>();
  private final Map<String, Map<String, Long>> byGradeCode = new HashMap<>();

  public void accumulate(List<String> keywords, String gradeCode) {

    for (String keyword : keywords) {

      totalKeywordCount.merge(keyword, 1L, Long::sum);

      byGradeCode
          .computeIfAbsent(gradeCode, k -> new HashMap<>())
          .merge(keyword, 1L, Long::sum);
    }
  }

  public Map<String, Long> getTotalKeywordCount() {
    return totalKeywordCount;
  }

  public Map<String, Map<String, Long>> getByGradeCode() {
    return byGradeCode;
  }
}