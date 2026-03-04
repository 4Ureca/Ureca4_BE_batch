package com.uplus.batch.jobs.daily_report.dto;

import com.uplus.batch.jobs.daily_report.step.keyword_rank.KeywordCount;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import java.time.LocalDate;
import java.util.List;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "daily_report_snapshot")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DailyReportSnapshot {

  private LocalDate date;

  private List<KeywordCount> topKeywords;

  private List<GradeSnapshot> byGradeCode;
}