package com.uplus.batch.jobs.monthly_report.config;

import com.uplus.batch.jobs.weekly_report.step.performance.PerformanceTasklet;
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
 * 월간 리포트 배치 Job 설정
 *
 * PerformanceTasklet을 재사용하며, targetCollection만 monthly_report_snapshot으로 변경.
 *
 * 실행:
 *   curl -X POST "http://localhost:8081/api/jobs/monthly-performance?startDate=2025-01-01&endDate=2025-01-31"
 */
@Configuration
@RequiredArgsConstructor
public class MonthlyReportJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public Job monthlyPerformanceJob(Step monthlyPerformanceStep) {
        return new JobBuilder("monthlyPerformanceJob", jobRepository)
                .start(monthlyPerformanceStep)
                .build();
    }

    @Bean
    public Step monthlyPerformanceStep(PerformanceTasklet performanceTasklet) {
        return new StepBuilder("monthlyPerformanceStep", jobRepository)
                .tasklet(performanceTasklet, transactionManager)
                .build();
    }
}
