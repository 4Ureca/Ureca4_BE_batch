package com.uplus.batch.jobs.monthly_agent_report;

import com.uplus.batch.jobs.daily_agent_report.entity.CategoryRanking;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot;
import com.uplus.batch.jobs.monthly_agent_report.entity.MonthlyAgentReportSnapshot;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Comparator;
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
public class MonthlyAgentReportProcessor implements
    ItemProcessor<Long, MonthlyAgentReportSnapshot> {

  private final MongoTemplate mongoTemplate;

  @Override
  public MonthlyAgentReportSnapshot process(Long agentId) {
    // [테스트용] 2025년 1월 데이터 집계 (1일 ~ 31일)
    LocalDate startAt = LocalDate.of(2025, 1, 1);
    LocalDate endAt = LocalDate.of(2025, 1, 31);

    // 실운영 시 자동 계산:
    // LocalDate startAt = LocalDate.now().minusMonths(1).withDayOfMonth(1);
    // LocalDate endAt = LocalDate.now().minusMonths(1).with(TemporalAdjusters.lastDayOfMonth());

    // 1. 해당 월의 모든 일별 스냅샷 조회
    Query query = new Query(
        Criteria.where("agentId").is(agentId)
            .and("startAt").gte(startAt).lte(endAt)
    );
    List<DailyAgentReportSnapshot> dailySnapshots = mongoTemplate.find(query, DailyAgentReportSnapshot.class);

    if (dailySnapshots.isEmpty()) return null;

    // 2. 데이터 합산 (건수 및 카테고리 랭킹)
    long totalConsultCount = 0;
    double totalDurationSum = 0;
    double totalSatisfactionSum = 0;

    long totalSurveyTotalCount = 0;    // 월간 설문 요청 총합
    long totalSurveyResponseCount = 0; // 월간 설문 응답 총합

    Map<String, CategoryRanking> combinedRankings = new HashMap<>();

    for (DailyAgentReportSnapshot day : dailySnapshots) {
      long dayCount = day.getConsultCount();
      totalConsultCount += day.getConsultCount();

      // 가중치 계산을 위한 합산 (건수가 0인 날은 제외됨)
      totalDurationSum += (day.getAvgDurationMinutes() * dayCount);
      totalSatisfactionSum += (day.getCustomerSatisfactionAnalysis().getSatisfactionScore() * dayCount);

      // 일별 설문 카운트 합산 (가중 평균용)
      if (day.getCustomerSatisfactionAnalysis() != null) {
        totalSurveyTotalCount += day.getCustomerSatisfactionAnalysis().getSurveyTotalCount();
        totalSurveyResponseCount += day.getCustomerSatisfactionAnalysis().getSurveyResponseCount();
      }

      // 처리 카테고리 순위 및 건수
      for (CategoryRanking r : day.getCategoryRanking()) {
        CategoryRanking existing = combinedRankings.getOrDefault(r.getCode(),
            new CategoryRanking(r.getCode(), r.getLarge(), r.getMedium(), 0, 0));
        existing.setCount(existing.getCount() + r.getCount());
        combinedRankings.put(r.getCode(), existing);
      }
    }

    // 3. 최종 월별 지표 산출
    double monthlyAvgDuration = totalConsultCount > 0 ? totalDurationSum / totalConsultCount : 0;
    double monthlyAvgSatisfaction = totalConsultCount > 0 ? totalSatisfactionSum / totalConsultCount : 0;

    // 월간 평균 응답률 계산
    double monthlyAvgResponseRate = totalSurveyTotalCount > 0
        ? (double) totalSurveyResponseCount / totalSurveyTotalCount * 100.0 : 0;

    // 4. 카테고리 재정렬 및 순위 부여
    List<CategoryRanking> sortedRankings = combinedRankings.values().stream()
        .sorted(Comparator.comparingLong(CategoryRanking::getCount).reversed())
        .collect(Collectors.toList());

    for (int i = 0; i < sortedRankings.size(); i++) {
      sortedRankings.get(i).setRank(i + 1);
    }

    // 5. 월별 결과 생성
    return MonthlyAgentReportSnapshot.builder()
        .agentId(agentId)
        .startAt(startAt)
        .endAt(endAt)
        .consultCount(totalConsultCount)
        .avgDurationMinutes(monthlyAvgDuration) // 주간 가중 평균 소요 시간
        .customerSatisfactionAnalysis(
            MonthlyAgentReportSnapshot.CustomerSatisfactionAnalysis.builder()
                .satisfactionScore(monthlyAvgSatisfaction)
                .responseRate(monthlyAvgResponseRate)
                .build()
        )
        .categoryRanking(sortedRankings)
        .build();
  }
}