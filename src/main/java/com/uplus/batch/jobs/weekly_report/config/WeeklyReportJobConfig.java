package com.uplus.batch.jobs.weekly_report.config;

import com.uplus.batch.jobs.common.step.keyword.KeywordStatsTasklet;
import com.uplus.batch.jobs.weekly_report.step.performance.PerformanceTasklet;
import com.uplus.batch.jobs.weekly_report.step.admin.WeeklySubscriptionStatsTasklet;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * 주간 관리자 리포트 통합 배치 Job 설정
 *
 * Step 실행 순서:
 *   1. weeklyPerformanceStep        — 전체 상담 성과 집계
 *   2. weeklyKeywordStatsStep       — 키워드 집계
 *   3. weeklySubscriptionStatsStep  — 구독상품 선호도 집계
 *
 * 실행:
 *   curl -X GET "http://localhost:8081/api/jobs/run-weekly-batch?startDate=2025-01-13&endDate=2025-01-19"
 */
@Configuration
@RequiredArgsConstructor
public class WeeklyReportJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final MongoTemplate mongoTemplate;
    private final WeeklySubscriptionStatsTasklet weeklySubscriptionStatsTasklet;

    @Bean
    public Job weeklyAdminReportJob(Step weeklyPerformanceStep) {
        return new JobBuilder("weeklyAdminReportJob", jobRepository)
                .start(weeklyPerformanceStep)
                .next(weeklyKeywordStatsStep())
                .next(weeklySubscriptionStatsStep())
                .build();
    }

    @Bean
    public Step weeklyPerformanceStep(PerformanceTasklet performanceTasklet) {
        return new StepBuilder("weeklyPerformanceStep", jobRepository)
                .tasklet(performanceTasklet, transactionManager)
                .build();
    }

    @Bean
    public Step weeklyKeywordStatsStep() {
        return new StepBuilder("weeklyKeywordStatsStep", jobRepository)
                .tasklet(new KeywordStatsTasklet(mongoTemplate, "weekly_report_snapshot"), transactionManager)
                .build();
    }

    @Bean
    public Step weeklySubscriptionStatsStep() {
        return new StepBuilder("weeklySubscriptionStatsStep", jobRepository)
                .tasklet(weeklySubscriptionStatsTasklet, transactionManager)
                .build();
    }
}
