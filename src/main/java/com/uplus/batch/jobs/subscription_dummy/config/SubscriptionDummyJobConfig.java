package com.uplus.batch.jobs.subscription_dummy.config;

import com.uplus.batch.jobs.subscription_dummy.dto.CustomerInfo;
import com.uplus.batch.jobs.subscription_dummy.tasklet.FamilyCombineTasklet;
import com.uplus.batch.jobs.subscription_dummy.dto.CustomerSubscriptionPlan;
import com.uplus.batch.jobs.subscription_dummy.processor.SubscriptionProcessor;
import com.uplus.batch.jobs.subscription_dummy.writer.SubscriptionCompositeWriter;
import java.sql.ResultSet;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.beans.factory.annotation.Value;

/**
 * 고객 구독 더미데이터 생성 배치 Job
 *
 * ─── 대상 테이블 ───
 * 45. customer_contracts        (고객 계약 결합상품 정의)
 * 38. customer_subscription_mobile  (고객 모바일 구독)
 * 39. customer_subscription_home    (고객 인터넷/TV 구독)
 * 37. customer_subscription_additional (고객 부가서비스 구독)
 *
 * ─── 흐름 ───
 * Reader: customers 테이블에서 기존 고객 정보 읽기
 * Processor: 비즈니스 규칙에 따라 구독 플랜 생성
 * Writer: 4개 테이블에 순차 INSERT (contract → mobile → home → additional)
 *
 * ─── 실행 ───
 * gradlew.bat bootRun --args="--spring.batch.job.name=subscriptionDummyJob runId=[버전]"
 */
@Configuration
@RequiredArgsConstructor
public class SubscriptionDummyJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;

    @Bean
    public Job subscriptionDummyJob(Step subscriptionStep, Step familyCombineStep) {
        return new JobBuilder("subscriptionDummyJob", jobRepository)
                .start(subscriptionStep)       // Step 1: 고객별 구독 INSERT
                .next(familyCombineStep)        // Step 2: 20% 가족결합 UPDATE
                .build();
    }

    @Bean
    public Step subscriptionStep(
            JdbcCursorItemReader<CustomerInfo> customerReader,
            SubscriptionProcessor processor,
            SubscriptionCompositeWriter writer) {

        return new StepBuilder("subscriptionStep", jobRepository)
                .<CustomerInfo, CustomerSubscriptionPlan>chunk(500, transactionManager)
                .reader(customerReader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public Step familyCombineStep(FamilyCombineTasklet familyCombineTasklet) {
        return new StepBuilder("familyCombineStep", jobRepository)
                .tasklet(familyCombineTasklet, transactionManager)
                .build();
    }

    /**
     * 기존 customers 테이블에서 고객 정보 읽기
     * (customer_id, birth_date, grade_code, created_at)
     */
    @Bean
    @StepScope
    public JdbcCursorItemReader<CustomerInfo> customerReader(
        @Value("#{jobParameters['startId']}") Long startId,
        @Value("#{jobParameters['endId']}") Long endId
    ) {

      Long from = (startId != null) ? startId : 0L;
      Long to   = (endId != null)   ? endId   : Long.MAX_VALUE;

      String sql = """
        SELECT customer_id, birth_date, grade_code, created_at
        FROM customers
        WHERE customer_id BETWEEN ? AND ?
        ORDER BY customer_id
    """;

      return new JdbcCursorItemReaderBuilder<CustomerInfo>()
          .name("customerReader")
          .dataSource(dataSource)
          .sql(sql)
          .preparedStatementSetter(ps -> {
            ps.setLong(1, from);
            ps.setLong(2, to);
          })
          .rowMapper((rs, rowNum) ->
              CustomerInfo.builder()
                  .customerId(rs.getLong("customer_id"))
                  .birthDate(rs.getDate("birth_date").toLocalDate())
                  .gradeCode(rs.getString("grade_code"))
                  .createdAt(rs.getTimestamp("created_at").toLocalDateTime())
                  .build()
          )
          .build();
    }
}