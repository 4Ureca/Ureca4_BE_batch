package com.uplus.batch.jobs.customer_dummy;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBatchTest
@SpringBootTest
@ActiveProfiles("test")
class CustomerDummyJobTest {

  @Autowired
  private JobLauncherTestUtils jobLauncherTestUtils;

  @Test
  void 고객더미_100건_생성_테스트() throws Exception {

    JobExecution execution =
        jobLauncherTestUtils.launchJob(
            new JobParametersBuilder()
                .addLong("count", 100L)
                .addLong("time", System.currentTimeMillis())
                .toJobParameters()
        );

    assertThat(execution.getStatus())
        .isEqualTo(BatchStatus.COMPLETED);
  }
}