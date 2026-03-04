package com.uplus.batch.jobs.daily_report.dto;

import com.uplus.batch.jobs.daily_report.step.keyword_rank.KeywordCount;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GradeSnapshot {
  private String gradeCode;
  private List<KeywordCount> keywords;
}
