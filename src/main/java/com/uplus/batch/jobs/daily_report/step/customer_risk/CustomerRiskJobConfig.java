package com.uplus.batch.jobs.daily_report.step.customer_risk;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * 고객 특이사항 집계 배치 Job 설정 (단일 Step)
 *
 * 실행:
 *   curl -X POST "http://localhost:8081/api/analysis/jobs/customer-risk?targetDate=2025-01-15"
 *
 * 나중에 dailyReportJob에 합칠 때 이 Config 제거하고
 * DailyReportJobConfig에서 customerRiskStep만 가져다 쓰면 됨.
 */
@Configuration
@RequiredArgsConstructor
public class CustomerRiskJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public Job customerRiskJob(Step customerRiskStep) {
        return new JobBuilder("customerRiskJob", jobRepository)
                .start(customerRiskStep)
                .build();
    }

    @Bean
    public Step customerRiskStep(CustomerRiskTasklet customerRiskTasklet) {
        return new StepBuilder("customerRiskStep", jobRepository)
                .tasklet(customerRiskTasklet, transactionManager)
                .build();
    }
}