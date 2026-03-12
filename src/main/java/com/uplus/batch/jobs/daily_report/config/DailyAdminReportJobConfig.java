package com.uplus.batch.jobs.daily_report.config;

import com.uplus.batch.jobs.daily_report.step.customer_risk.CustomerRiskTasklet;
import com.uplus.batch.jobs.daily_report.step.hourly_consult.HourlyConsultTasklet;
import com.uplus.batch.jobs.daily_report.step.keyword_rank.KeywordRankTasklet;
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
 * 일별 관리자 리포트 통합 배치 Job 설정
 *
 * Step 실행 순서:
 *   1. dailyPerformanceStep     — 전체 상담 성과 집계
 *   2. dailyKeywordStep         — 키워드 집계
 *   3. dailyCustomerRiskStep    — 고객 특이사항(리스크) 집계
 *   4. dailyHourlyConsultStep   — 시간대별 이슈 트렌드 집계
 *
 * 실행:
 *   curl -X POST "http://localhost:8081/api/jobs/run-daily-batch?date=2025-01-18"
 */
@Configuration
@RequiredArgsConstructor
public class DailyAdminReportJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public Job dailyAdminReportJob(
            Step dailyPerformanceStep,
            Step dailyKeywordStep,
            Step dailyCustomerRiskStep,
            Step dailyHourlyConsultStep) {
        return new JobBuilder("dailyAdminReportJob", jobRepository)
                .start(dailyPerformanceStep)
                .next(dailyKeywordStep)
                .next(dailyCustomerRiskStep)
                .next(dailyHourlyConsultStep)
                .build();
    }

    @Bean
    public Step dailyPerformanceStep(PerformanceTasklet performanceTasklet) {
        return new StepBuilder("dailyPerformanceStep", jobRepository)
                .tasklet(performanceTasklet, transactionManager)
                .build();
    }

    @Bean
    public Step dailyKeywordStep(KeywordRankTasklet keywordRankTasklet) {
        return new StepBuilder("dailyKeywordStep", jobRepository)
                .tasklet(keywordRankTasklet, transactionManager)
                .build();
    }

    @Bean
    public Step dailyCustomerRiskStep(CustomerRiskTasklet customerRiskTasklet) {
        return new StepBuilder("dailyCustomerRiskStep", jobRepository)
                .tasklet(customerRiskTasklet, transactionManager)
                .build();
    }

    @Bean
    public Step dailyHourlyConsultStep(HourlyConsultTasklet hourlyConsultTasklet) {
        return new StepBuilder("dailyHourlyConsultStep", jobRepository)
                .tasklet(hourlyConsultTasklet, transactionManager)
                .build();
    }
}
