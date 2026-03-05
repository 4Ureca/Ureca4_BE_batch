package com.uplus.batch.jobs.monthly_reports.step.agent;

import com.uplus.batch.jobs.daily_agent_report.entity.CategoryRanking;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot;
import com.uplus.batch.jobs.monthly_agent_report.MonthlyAgentReportProcessor;
import com.uplus.batch.jobs.monthly_agent_report.entity.MonthlyAgentReportSnapshot;
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
    String agentId = "agent-001";

    // 1월 1일 데이터: 요금조회(10건)
    DailyAgentReportSnapshot day1 = DailyAgentReportSnapshot.builder()
        .agentId(agentId)
        .consultCount(10)
        .categoryRanking(List.of(
            new CategoryRanking("FEE_01", "요금", "조회", 10, 1)
        )).build();

    // 1월 2일 데이터: 요금조회(5건), 기기변경(20건) -> 기기변경이 1위가 되어야 함
    DailyAgentReportSnapshot day2 = DailyAgentReportSnapshot.builder()
        .agentId(agentId)
        .consultCount(25)
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

    MonthlyAgentReportSnapshot result = processor.process("unknown-agent");

    assertThat(result).isNull();
  }
}