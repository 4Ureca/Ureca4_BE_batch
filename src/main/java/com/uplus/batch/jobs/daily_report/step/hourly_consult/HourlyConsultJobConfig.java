package com.uplus.batch.jobs.daily_report.step.hourly_consult;

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
 * 시간대별 이슈 트렌드 집계 Job/Step 설정
 *
 * 단독 실행용 — 나중에 DailyReportJobConfig에 통합 예정
 *
 * curl -X POST "http://localhost:8081/api/jobs/hourly-consult?targetDate=2026-03-03&slot=09-12"
 */
@Configuration
@RequiredArgsConstructor
public class HourlyConsultJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public Job hourlyConsultJob(Step hourlyConsultStep) {
        return new JobBuilder("hourlyConsultJob", jobRepository)
                .start(hourlyConsultStep)
                .build();
    }

    @Bean
    public Step hourlyConsultStep(HourlyConsultTasklet hourlyConsultTasklet) {
        return new StepBuilder("hourlyConsultStep", jobRepository)
                .tasklet(hourlyConsultTasklet, transactionManager)
                .build();
    }
}
