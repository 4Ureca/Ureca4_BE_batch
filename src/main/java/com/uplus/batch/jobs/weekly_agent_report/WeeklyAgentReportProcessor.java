package com.uplus.batch.jobs.weekly_agent_report;

import com.uplus.batch.jobs.daily_agent_report.entity.CategoryRanking;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot;
import com.uplus.batch.jobs.weekly_agent_report.entity.WeeklyAgentReportSnapshot;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class WeeklyAgentReportProcessor implements ItemProcessor<String, WeeklyAgentReportSnapshot> {

  private final MongoTemplate mongoTemplate;

  @Override
  public WeeklyAgentReportSnapshot process(String agentId) {
    // [테스트용] 지난주 월요일 ~ 일요일 고정 (2025-01-13 ~ 2025-01-19)
    LocalDate startAt = LocalDate.of(2025, 1, 13);
    LocalDate endAt = LocalDate.of(2025, 1, 19);

    // [운영용]] 자동 계산 로직:
    // LocalDate now = LocalDate.now();
    // LocalDate startAt = now.minusWeeks(1).with(DayOfWeek.MONDAY);
    // LocalDate endAt = now.minusWeeks(1).with(DayOfWeek.SUNDAY);

    // 1. 해당 기간의 일별 스냅샷들을 가져옴
    Query query = new Query(
        Criteria.where("agentId").is(agentId)
            .and("startAt").gte(startAt).lte(endAt)
    );
    List<DailyAgentReportSnapshot> dailySnapshots = mongoTemplate.find(query, DailyAgentReportSnapshot.class);

    if (dailySnapshots.isEmpty()) return null;

    // 2. 데이터 합산 (건수 합산 및 카테고리 랭킹 재집계)
    long totalConsultCount = 0;
    Map<String, CategoryRanking> combinedRankings = new HashMap<>();

    for (DailyAgentReportSnapshot day : dailySnapshots) {
      totalConsultCount += day.getConsultCount();

      for (CategoryRanking r : day.getCategoryRanking()) {
        CategoryRanking existing = combinedRankings.getOrDefault(r.getCode(),
            new CategoryRanking(r.getCode(), r.getLarge(), r.getMedium(), 0, 0)); //, r.getSmall()
        existing.setCount(existing.getCount() + r.getCount());
        combinedRankings.put(r.getCode(), existing);
      }
    }

    // 3. 카테고리 재정렬 및 순위 부여
    List<CategoryRanking> sortedRankings = combinedRankings.values().stream()
        .sorted(Comparator.comparingLong(CategoryRanking::getCount).reversed())
        .collect(Collectors.toList());

    for (int i = 0; i < sortedRankings.size(); i++) {
      sortedRankings.get(i).setRank(i + 1);
    }

    // 4. 주별 결과 생성
    return WeeklyAgentReportSnapshot.builder()
        .agentId(agentId)
        .startAt(startAt)
        .endAt(endAt)
        .consultCount(totalConsultCount)
        .categoryRanking(sortedRankings)
        .build();
  }
}
