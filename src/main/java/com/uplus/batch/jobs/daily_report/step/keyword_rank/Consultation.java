package com.uplus.batch.jobs.daily_report.step.keyword_rank;

import lombok.Data;
import java.time.LocalDateTime;

@Data
public class Consultation {

  private Long id;
  private String content;
  private String gradeCode;
  private LocalDateTime consultDate;
}
