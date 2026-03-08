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
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@StepScope
public class WeeklyAgentReportProcessor implements ItemProcessor<Long, WeeklyAgentReportSnapshot> {

  private final MongoTemplate mongoTemplate;

  @Override
  public WeeklyAgentReportSnapshot process(Long agentId) {
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
    double totalDurationSum = 0; // (평균 시간 * 건수)의 합산
    double totalSatisfactionSum = 0; // (만족도 * 건수)의 합산
    Map<String, CategoryRanking> combinedRankings = new HashMap<>();

    for (DailyAgentReportSnapshot day : dailySnapshots) {
      long dayCount = day.getConsultCount();
      totalConsultCount += day.getConsultCount();

      // 가중치 계산을 위한 합산 (건수가 0인 날은 제외됨)
      totalDurationSum += (day.getAvgDurationMinutes() * dayCount);
      totalSatisfactionSum += (day.getCustomerSatisfaction() * dayCount);

      // 카테고리 랭킹 합산 로직
      for (CategoryRanking r : day.getCategoryRanking()) {
        CategoryRanking existing = combinedRankings.getOrDefault(r.getCode(),
            new CategoryRanking(r.getCode(), r.getLarge(), r.getMedium(), 0, 0)); //, r.getSmall()
        existing.setCount(existing.getCount() + r.getCount());
        combinedRankings.put(r.getCode(), existing);
      }
    }

    // 3. 최종 평균 지표 산출 (전체 건수로 나눔)
    double weeklyAvgDuration = totalConsultCount > 0 ? totalDurationSum / totalConsultCount : 0;
    double weeklyAvgSatisfaction = totalConsultCount > 0 ? totalSatisfactionSum / totalConsultCount : 0;

    // 4. 카테고리 재정렬 및 순위 부여
    List<CategoryRanking> sortedRankings = combinedRankings.values().stream()
        .sorted(Comparator.comparingLong(CategoryRanking::getCount).reversed())
        .collect(Collectors.toList());

    for (int i = 0; i < sortedRankings.size(); i++) {
      sortedRankings.get(i).setRank(i + 1);
    }

    // 5. 주별 결과 생성
    return WeeklyAgentReportSnapshot.builder()
        .agentId(agentId)
        .startAt(startAt)
        .endAt(endAt)
        .consultCount(totalConsultCount)
        .avgDurationMinutes(weeklyAvgDuration) // 주간 가중 평균 소요 시간
        .customerSatisfaction(weeklyAvgSatisfaction) // 주간 가중 평균 만족도
        .categoryRanking(sortedRankings)
        .build();
  }
}
