package com.uplus.batch.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/dummy")
@RequiredArgsConstructor
public class DummyBatchController {

  private final JobLauncher jobLauncher;
  private final Job customerDummyJob;
  private final Job subscriptionDummyJob;
  private final Job consultationDummyJob;
  private final Job consultationSummaryDummyJob;

  @PostMapping("/customers")
  public ResponseEntity<String> runCustomerDummy(
      @RequestParam(defaultValue = "1000") int count
  ) throws Exception {

    JobParameters jobParameters = new JobParametersBuilder()
        .addLong("runId", System.currentTimeMillis())
        .addLong("count", (long) count)
        .toJobParameters();

    jobLauncher.run(customerDummyJob, jobParameters);

    return ResponseEntity.ok("Customer dummy job started");
  }

  @PostMapping("/subscriptions")
  public ResponseEntity<String> runSubscriptionDummy(
      @RequestParam Long startId,
      @RequestParam Long endId
  ) throws Exception {

    JobParameters jobParameters = new JobParametersBuilder()
        .addLong("runId", System.currentTimeMillis())
        .addLong("startId", startId)
        .addLong("endId", endId)
        .toJobParameters();

    jobLauncher.run(subscriptionDummyJob, jobParameters);

    return ResponseEntity.ok("Subscription dummy job started");
  }

  @PostMapping("/summary-dummy")
  public ResponseEntity<String> run(
      @RequestParam long startId,
      @RequestParam long endId
  ) throws Exception {

    if (startId > endId) {
      return ResponseEntity.badRequest()
          .body("startId must be less than or equal to endId");
    }

    JobParameters jobParameters =
        new JobParametersBuilder()
            .addLong("startId", startId)
            .addLong("endId", endId)
            .addLong("runId", System.currentTimeMillis()) // 중복 실행 방지
            .toJobParameters();

    jobLauncher.run(consultationSummaryDummyJob, jobParameters);

    return ResponseEntity.ok(
        "Batch started: startId=" + startId + ", endId=" + endId
    );
  }

  /**
   * 상담 결과 더미데이터 생성 (employees, customers, consultation_category_policy 데이터 필요)
   * curl -X POST "http://localhost:8081/dummy/consultations?count=50"
   */
  @PostMapping("/consultations")
  public ResponseEntity<String> runConsultationDummy(
      @RequestParam(defaultValue = "10") int count
  ) throws Exception {

    JobParameters jobParameters = new JobParametersBuilder()
        .addLong("runId", System.currentTimeMillis())
        .addLong("count", (long) count)
        .toJobParameters();

    jobLauncher.run(consultationDummyJob, jobParameters);

    return ResponseEntity.ok("Consultation dummy job started (" + count + "건)");
  }
}