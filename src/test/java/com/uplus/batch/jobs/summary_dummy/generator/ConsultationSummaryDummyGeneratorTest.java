package com.uplus.batch.jobs.summary_dummy.generator;

import com.uplus.batch.common.dummy.dto.CacheDummy;
import com.uplus.batch.common.dummy.dto.RiskTypeDummyDto;
import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.jobs.summary_dummy.dto.ConsultationResultRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConsultationSummaryDummyGeneratorTest {

  private ConsultationSummaryDummyGenerator generator;

  @BeforeEach
  void setUp() {

    CacheDummy cache = new CacheDummy();

    cache.initialize(
        Map.of(
            "R1", new RiskTypeDummyDto("R1", "위험1")
        ),
        List.of("요금할인", "상품변경"),
        List.of("NEW", "CANCEL", "CHANGE", "RENEW"),
        List.of("HOME_A"),
        List.of("MOBILE_A"),
        List.of("ADD_A")
    );

    generator = new ConsultationSummaryDummyGenerator(cache);
  }

  private ConsultationResultRow createRow() {
    return ConsultationResultRow.builder()
        .consultId(1L)
        .createdAt(LocalDateTime.now())
        .channel("CALL")
        .durationSec(300)
        .empId(100L)
        .agentName("홍길동")
        .customerId(200L)
        .customerName("고객A")
        .customerType("INDIVIDUAL")
        .customerPhone("01012345678")
        .gradeCode("VIP")
        .birthDate(LocalDate.of(1990,1,1))
        .categoryCode("C01")
        .categoryLarge("요금")
        .categoryMedium("할인")
        .categorySmall("프로모션")
        .iamIssue("요금 문의")
        .iamAction("할인 적용")
        .iamMemo("상담 완료")
        .build();
  }

  @Test
  @DisplayName("정상 더미 문서 생성")
  void generate_success() {

    ConsultationSummary result = generator.generate(createRow());

    assertThat(result).isNotNull();
    assertThat(result.getConsultId()).isEqualTo(1L);
    assertThat(result.getAgent().getName()).isEqualTo("홍길동");
    assertThat(result.getCustomer().getAgeGroup()).isEqualTo("30대");
    assertThat(result.getCategory().getCode()).isEqualTo("C01");
    assertThat(result.getResultProducts()).isNotEmpty();
  }

  @Test
  @DisplayName("계약 타입이 없으면 예외 발생")
  void contractTypes_empty() {

    CacheDummy emptyCache = new CacheDummy();
    emptyCache.initialize(
        Map.of(),
        List.of(),
        List.of(),   // 계약 타입 없음
        List.of("HOME_A"),
        List.of("MOBILE_A"),
        List.of("ADD_A")
    );

    ConsultationSummaryDummyGenerator brokenGenerator =
        new ConsultationSummaryDummyGenerator(emptyCache);

    assertThrows(
        IllegalStateException.class,
        () -> brokenGenerator.generate(createRow())
    );
  }

  @Test
  @DisplayName("상품 코드가 없으면 예외 발생")
  void productCodes_empty() {

    CacheDummy emptyCache = new CacheDummy();
    emptyCache.initialize(
        Map.of(),
        List.of(),
        List.of("NEW"),
        List.of(),   // 상품 없음
        List.of(),
        List.of()
    );

    ConsultationSummaryDummyGenerator brokenGenerator =
        new ConsultationSummaryDummyGenerator(emptyCache);

    assertThrows(
        IllegalStateException.class,
        () -> brokenGenerator.generate(createRow())
    );
  }
}