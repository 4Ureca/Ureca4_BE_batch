package com.uplus.batch.jobs.monthly_reports.step.agent;

import com.uplus.batch.jobs.daily_agent_report.entity.CategoryRanking;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot;
import com.uplus.batch.jobs.monthly_agent_report.MonthlyAgentReportProcessor;
import com.uplus.batch.jobs.monthly_agent_report.entity.MonthlyAgentReportSnapshot;
import java.util.ArrayList;
import java.util.Collections;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("MonthlyAgentReportProcessor 로직 테스트")
class MonthlyAgentReportProcessorTest {

  @Mock
  private MongoTemplate mongoTemplate;

  @InjectMocks
  private MonthlyAgentReportProcessor processor;

  @Test
  @DisplayName("한 달치 일별 데이터 합산 및 엔티티 생성 테스트")
  void process_MonthlyAggregation_Success() {
    // 1. Given: 테스트용 상담사 ID와 일별 데이터 준비
    Long agentId = 101L;

    // 1월 1일 데이터: 요금조회(10건)
    DailyAgentReportSnapshot day1 = DailyAgentReportSnapshot.builder()
        .agentId(agentId)
        .consultCount(10)
        .customerSatisfactionAnalysis(
            DailyAgentReportSnapshot.CustomerSatisfactionAnalysis.builder()
                .satisfactionScore(4.5)
                .build()
        )
        .categoryRanking(List.of(
            new CategoryRanking("FEE_01", "요금", "조회", 10, 1)
        )).build();

    // 1월 2일 데이터: 요금조회(5건), 기기변경(20건) -> 기기변경이 1위가 되어야 함
    DailyAgentReportSnapshot day2 = DailyAgentReportSnapshot.builder()
        .agentId(agentId)
        .consultCount(25)
        .customerSatisfactionAnalysis(
            DailyAgentReportSnapshot.CustomerSatisfactionAnalysis.builder()
                .satisfactionScore(0.0) // 혹은 적절한 테스트 값
                .build()
        )
        .categoryRanking(List.of(
            new CategoryRanking("DEVICE_01", "기기", "변경", 20, 1),
            new CategoryRanking("FEE_01", "요금", "조회", 5, 2)
        )).build();

    // [중요] Processor의 find 인자 개수에 맞춰 스터빙 (현재 2개 사용 중)
    when(mongoTemplate.find(
        any(Query.class),
        eq(DailyAgentReportSnapshot.class)
    )).thenReturn(List.of(day1, day2));

    // 2. When: 월간 프로세서 실행
    MonthlyAgentReportSnapshot result = processor.process(agentId);

    // 3. Then: 결과 검증
    assertThat(result).isNotNull();
    assertThat(result.getConsultCount()).isEqualTo(35); // 10 + 25

    // 카테고리 합산 및 순위 확인 (DEVICE_01: 20건(1위), FEE_01: 15건(2위))
    List<CategoryRanking> rankings = result.getCategoryRanking();
    assertThat(rankings).hasSize(2);

    assertThat(rankings.get(0).getCode()).isEqualTo("DEVICE_01");
    assertThat(rankings.get(0).getCount()).isEqualTo(20);
    assertThat(rankings.get(0).getRank()).isEqualTo(1);

    assertThat(rankings.get(1).getCode()).isEqualTo("FEE_01");
    assertThat(rankings.get(1).getCount()).isEqualTo(15);
    assertThat(rankings.get(1).getRank()).isEqualTo(2);
  }

  @Test
  @DisplayName("데이터가 없는 경우 null 반환 확인")
  void process_NoData_ReturnsNull() {
    when(mongoTemplate.find(
        any(Query.class),
        eq(DailyAgentReportSnapshot.class)
    )).thenReturn(List.of());

    // ID를 숫자로 변경
    MonthlyAgentReportSnapshot result = processor.process(999L);

    assertThat(result).isNull();
  }

  // 전체 상담 성과 테스트
  @Test
  @DisplayName("월별 보고서 가중평균 및 만족도 집계 테스트")
  void 월별_보고서_지표_계산_테스트() {
    // given
    Long agentId = 101L;

    // 1월 1일 데이터 (10건, 만족도 4.0)
    DailyAgentReportSnapshot day1 = DailyAgentReportSnapshot.builder()
        .consultCount(10)
        .avgDurationMinutes(6.0)
        .customerSatisfactionAnalysis(
            DailyAgentReportSnapshot.CustomerSatisfactionAnalysis.builder()
                .satisfactionScore(4.0)
                .build()
        )
        .categoryRanking(new ArrayList<>())
        .build();

    // 1월 15일 데이터 (20건, 만족도 1.0 - 낮은 점수 케이스 반영)
    DailyAgentReportSnapshot day2 = DailyAgentReportSnapshot.builder()
        .consultCount(20)
        .avgDurationMinutes(9.0)
        .customerSatisfactionAnalysis(
            DailyAgentReportSnapshot.CustomerSatisfactionAnalysis.builder()
                .satisfactionScore(1.0)
                .build()
        )
        .categoryRanking(new ArrayList<>())
        .build();

    when(mongoTemplate.find(any(Query.class), eq(DailyAgentReportSnapshot.class)))
        .thenReturn(List.of(day1, day2));

    // when
    MonthlyAgentReportSnapshot result = processor.process(agentId);

    // then
    // 평균 시간 가중치: (6.0*10 + 9.0*20) / 30 = 8.0분
    assertEquals(8.0, result.getAvgDurationMinutes());

    // 만족도 가중치: (4.0*10 + 1.0*20) / 30 = 2.0점
    assertEquals(2.0, result.getCustomerSatisfactionAnalysis().getSatisfactionScore(), 0.01);

    // 전체 건수 합산: 10 + 20 = 30건
    assertEquals(30, result.getConsultCount());
  }

  @Test
  @DisplayName("해당 월에 일별 데이터가 없는 경우 null 반환 테스트")
  void 월별_데이터_부재_테스트() {

    Long agentId = 101L;

    // given
    when(mongoTemplate.find(any(Query.class), eq(DailyAgentReportSnapshot.class)))
        .thenReturn(Collections.emptyList());

    // when
    MonthlyAgentReportSnapshot result = processor.process(agentId);

    // then
    assertNull(result);
  }
}