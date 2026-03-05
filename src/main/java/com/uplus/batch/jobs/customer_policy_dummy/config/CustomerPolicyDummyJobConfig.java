package com.uplus.batch.jobs.customer_policy_dummy.config;

import com.uplus.batch.jobs.customer_policy_dummy.tasklet.CustomerPolicyDummyTasklet;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class CustomerPolicyDummyJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final CustomerPolicyDummyTasklet customerPolicyDummyTasklet;

    @Bean
    public Step customerPolicyDummyStep() {
        return new StepBuilder("customerPolicyDummyStep", jobRepository)
            .tasklet(customerPolicyDummyTasklet, transactionManager)
            .build();
    }

    @Bean
    public Job customerPolicyDummyJob() {
        return new JobBuilder("customerPolicyDummyJob", jobRepository)
            .start(customerPolicyDummyStep())
            .build();
    }
}