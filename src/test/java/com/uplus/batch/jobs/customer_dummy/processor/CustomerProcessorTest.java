package com.uplus.batch.jobs.customer_dummy.processor;
import static org.assertj.core.api.Assertions.assertThat;

import com.uplus.batch.domain.customer.dto.CustomerRow;
import org.junit.jupiter.api.RepeatedTest;

class CustomerProcessorTest {

  private final CustomerProcessor processor = new CustomerProcessor();

  @RepeatedTest(30)
  void 고객_더미데이터_제약조건_검증_테스트() throws Exception {

    CustomerRow row = processor.process(1);

    assertThat(row).isNotNull();
    assertThat(row.getIdentificationNum()).isNotBlank();
    assertThat(row.getName()).isNotBlank();
    assertThat(row.getCustomerType()).isIn("개인", "법인");

    if ("개인".equals(row.getCustomerType())) {
      assertThat(row.getGender()).isIn("M", "F");
    } else {
      assertThat(row.getGender()).isNull();
    }

    assertThat(row.getGradeCode()).isIn("DIAMOND", "VIP", "VVIP");
    assertThat(row.getPreferredContact()).isIn("call", "email", "push");

    assertThat(row.getEmail()).contains("@");
    assertThat(row.getPhone()).startsWith("010");

    assertThat(row.getBirthDate().getYear())
        .isBetween(1960, 2005);

    assertThat(row.getCreatedAt()).isNotNull();
  }
}