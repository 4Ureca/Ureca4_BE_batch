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
}