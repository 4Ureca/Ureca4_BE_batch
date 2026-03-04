package com.uplus.batch.jobs.daily_report.step.keyword_rank;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordCount {
  private String keyword;
  private Long count;
}
