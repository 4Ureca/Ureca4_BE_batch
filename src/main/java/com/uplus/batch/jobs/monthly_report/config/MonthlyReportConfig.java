package com.uplus.batch.jobs.monthly_report.config;

import com.uplus.batch.jobs.common.step.keyword.KeywordStatsTasklet;
import com.uplus.batch.jobs.monthly_report.step.admin.ChurnDefenseStatsTasklet;
import com.uplus.batch.jobs.monthly_report.step.admin.MonthlySubscriptionStatsTasklet;
import com.uplus.batch.jobs.monthly_report.step.customer_risk.MonthlyCustomerRiskTasklet;
import com.uplus.batch.jobs.weekly_report.step.performance.PerformanceTasklet;
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
 * 월간 관리자 리포트 통합 배치 Job 설정
 *
 * Step 실행 순서:
 *   1. monthlyPerformanceStep        — 전체 상담 성과 집계
 *   2. monthlyKeywordStatsStep       — 키워드 집계
 *   3. monthlySubscriptionStatsStep  — 구독상품 선호도 집계
 *   4. monthlyChurnDefenseStep       — 이탈 방어 분석
 *   5. monthlyCustomerRiskStep       — 고객 특이사항 집계
 *
 * 실행:
 *   curl -X GET "http://localhost:8081/api/jobs/run-monthly-batch?startDate=2025-01-01&endDate=2025-01-31"
 */
@Configuration
@RequiredArgsConstructor
public class MonthlyReportConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final MongoTemplate mongoTemplate;
    private final MonthlySubscriptionStatsTasklet monthlySubscriptionStatsTasklet;
    private final MonthlyCustomerRiskTasklet monthlyCustomerRiskTasklet;
    private final ChurnDefenseStatsTasklet churnDefenseStatsTasklet;

    @Bean
    public Job monthlyAdminReportJob(Step monthlyPerformanceStep) {
        return new JobBuilder("monthlyAdminReportJob", jobRepository)
                .start(monthlyPerformanceStep)
                .next(monthlyKeywordStatsStep())
                .next(monthlySubscriptionStatsStep())
                .next(monthlyChurnDefenseStep())
                .next(monthlyCustomerRiskStep())
                .build();
    }

    @Bean
    public Step monthlyPerformanceStep(PerformanceTasklet performanceTasklet) {
        return new StepBuilder("monthlyPerformanceStep", jobRepository)
                .tasklet(performanceTasklet, transactionManager)
                .build();
    }

    @Bean
    public Step monthlyKeywordStatsStep() {
        return new StepBuilder("monthlyKeywordStatsStep", jobRepository)
                .tasklet(new KeywordStatsTasklet(mongoTemplate, "monthly_report_snapshot"), transactionManager)
                .build();
    }

    @Bean
    public Step monthlySubscriptionStatsStep() {
        return new StepBuilder("monthlySubscriptionStatsStep", jobRepository)
                .tasklet(monthlySubscriptionStatsTasklet, transactionManager)
                .build();
    }

    @Bean
    public Step monthlyChurnDefenseStep() {
        return new StepBuilder("monthlyChurnDefenseStep", jobRepository)
                .tasklet(churnDefenseStatsTasklet, transactionManager)
                .build();
    }

    @Bean
    public Step monthlyCustomerRiskStep() {
        return new StepBuilder("monthlyCustomerRiskStep", jobRepository)
                .tasklet(monthlyCustomerRiskTasklet, transactionManager)
                .build();
    }
}
