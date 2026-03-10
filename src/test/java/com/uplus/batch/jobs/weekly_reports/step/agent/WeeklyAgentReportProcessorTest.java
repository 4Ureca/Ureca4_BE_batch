package com.uplus.batch.jobs.weekly_reports.step.agent;

import com.uplus.batch.jobs.daily_agent_report.entity.CategoryRanking;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot;
import com.uplus.batch.jobs.weekly_agent_report.WeeklyAgentReportProcessor;
import com.uplus.batch.jobs.weekly_agent_report.entity.WeeklyAgentReportSnapshot;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import static org.assertj.core.api.Assertions.assertThat; // [해결] assertThat static import 추가
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
@DisplayName("WeeklyAgentReportProcessor 로직 테스트")
class WeeklyAgentReportProcessorTest {

  @Mock
  private MongoTemplate mongoTemplate;

  @InjectMocks
  private WeeklyAgentReportProcessor processor;

  @Test
  @DisplayName("7일치 일별 데이터가 있을 때 -> 정상 합산 및 순위 재정렬 확인")
  void process_WeeklyAggregation_Success() {
    // 1. Given: 가짜 일별 데이터(DailyAgentReportSnapshot) 생성
    Long agentId = 101L;

    // 첫 번째 날: CODE_A(5건), CODE_B(3건)
    DailyAgentReportSnapshot day1 = DailyAgentReportSnapshot.builder()
        .agentId(agentId)
        .consultCount(8)
        .customerSatisfactionAnalysis(
            DailyAgentReportSnapshot.CustomerSatisfactionAnalysis.builder()
                .satisfactionScore(4.0)
                .build()
        )
        .categoryRanking(List.of(
            new CategoryRanking("CODE_A", "대", "중A", 5, 1),
            new CategoryRanking("CODE_B", "대", "중B", 3, 2)
        )).build();

    // 두 번째 날: CODE_B(10건) -> 이제 CODE_B가 1위가 되어야 함
    DailyAgentReportSnapshot day2 = DailyAgentReportSnapshot.builder()
        .agentId(agentId)
        .consultCount(10)
        .customerSatisfactionAnalysis(
            DailyAgentReportSnapshot.CustomerSatisfactionAnalysis.builder()
                .satisfactionScore(4.0)
                .build()
        )
        .categoryRanking(List.of(
            new CategoryRanking("CODE_B", "대", "중B", 10, 1)
        )).build();

    // Mock 설정: daily_agent_report_snapshot 컬렉션에서 읽어옴
    when(mongoTemplate.find(
        any(Query.class),
        eq(DailyAgentReportSnapshot.class)
    )).thenReturn(List.of(day1, day2));

    // 2. When: 프로세서 실행
    WeeklyAgentReportSnapshot result = processor.process(agentId);

    // 3. Then: 결과 검증
    assertThat(result).isNotNull();
    assertThat(result.getConsultCount()).isEqualTo(18); // 8 + 10

    // 카테고리 합산 결과 확인 (CODE_B: 13건, CODE_A: 5건)
    List<CategoryRanking> rankings = result.getCategoryRanking();
    assertThat(rankings).hasSize(2);

    // 1순위가 CODE_B로 바뀌었는지 확인 (순위 재정렬 로직 검증)
    assertThat(rankings.get(0).getCode()).isEqualTo("CODE_B");
    assertThat(rankings.get(0).getCount()).isEqualTo(13);
    assertThat(rankings.get(0).getRank()).isEqualTo(1);

    assertThat(rankings.get(1).getCode()).isEqualTo("CODE_A");
    assertThat(rankings.get(1).getRank()).isEqualTo(2);
  }

  @Test
  @DisplayName("해당 기간에 일별 데이터가 전혀 없을 때 -> null 반환 확인")
  void process_NoData_ReturnsNull() {
    when(mongoTemplate.find(
        any(Query.class),
        eq(DailyAgentReportSnapshot.class)
    )).thenReturn(List.of());

    WeeklyAgentReportSnapshot result = processor.process(999L);

    assertThat(result).isNull();
  }

  // 전체 상담 성과 테스트
  @Test
  void 주별_보고서_가중평균_계산_테스트() {
    // given
    Long agentId = 101L;

    // 카테고리 랭킹이 null이 되지 않도록 빈 리스트를 넣음.
    DailyAgentReportSnapshot day1 = DailyAgentReportSnapshot.builder()
        .consultCount(10)
        .avgDurationMinutes(5.0)
        .customerSatisfactionAnalysis(
            DailyAgentReportSnapshot.CustomerSatisfactionAnalysis.builder()
                .satisfactionScore(4.0)
                .build()
        )
        .categoryRanking(new ArrayList<>()) // 이 부분이 누락되어 NPE 발생
        .build();

    DailyAgentReportSnapshot day2 = DailyAgentReportSnapshot.builder()
        .consultCount(20)
        .avgDurationMinutes(8.0)
        .customerSatisfactionAnalysis(
            DailyAgentReportSnapshot.CustomerSatisfactionAnalysis.builder()
                .satisfactionScore(5.0)
                .build()
        )
        .categoryRanking(new ArrayList<>())
        .build();

    when(mongoTemplate.find(any(Query.class), eq(DailyAgentReportSnapshot.class)))
        .thenReturn(List.of(day1, day2));

    // when
    WeeklyAgentReportSnapshot result = processor.process(agentId);

    // then
    assertEquals(7.0, result.getAvgDurationMinutes());
    // (4*10 + 5*20) / 30 = 4.666...
    assertEquals(4.66, result.getCustomerSatisfactionAnalysis().getSatisfactionScore(), 0.01);
  }

  @Test
  void 데이터가_없는_경우_null_반환_테스트() {
    // given
    when(mongoTemplate.find(any(Query.class), eq(DailyAgentReportSnapshot.class)))
        .thenReturn(Collections.emptyList()); // 빈 리스트 반환

    // when
    WeeklyAgentReportSnapshot result = processor.process(101L);

    // then
    assertNull(result); // 에러 없이 null이 나오는지 확인
  }

  @Test
  void 만족도_응답이_전혀_없는_경우_0점_반환_테스트() {
    // given
    DailyAgentReportSnapshot day1 = DailyAgentReportSnapshot.builder()
        .consultCount(10).avgDurationMinutes(5.0)
        .customerSatisfactionAnalysis(
            DailyAgentReportSnapshot.CustomerSatisfactionAnalysis.builder()
                .satisfactionScore(0.0)
                .build()
        ) // 만족도 응답 없음
        .categoryRanking(new ArrayList<>()).build();

    when(mongoTemplate.find(any(Query.class), eq(DailyAgentReportSnapshot.class)))
        .thenReturn(List.of(day1));

    // when
    WeeklyAgentReportSnapshot result = processor.process(101L);

    // then
    assertEquals(0.00, result.getCustomerSatisfactionAnalysis().getSatisfactionScore()); // 0으로 안전하게 떨어지는지 확인
  }

}