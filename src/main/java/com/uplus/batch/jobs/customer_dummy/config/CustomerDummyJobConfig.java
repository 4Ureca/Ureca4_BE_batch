package com.uplus.batch.jobs.customer_dummy.config;

import com.uplus.batch.domain.customer.dto.CustomerRow;
import com.uplus.batch.jobs.customer_dummy.processor.CustomerProcessor;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class CustomerDummyJobConfig {

  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;
  private final DataSource dataSource;

  @Bean
  public Job customerDummyJob(Step customerStep) {
    return new JobBuilder("customerDummyJob", jobRepository)
        .start(customerStep)
        .build();
  }

  @Bean
  public Step customerStep(ItemReader<Integer> reader,
      CustomerProcessor processor,
      JdbcBatchItemWriter<CustomerRow> writer) {

    return new StepBuilder("customerStep", jobRepository)
        .<Integer, CustomerRow>chunk(1000, transactionManager)
        .reader(reader)
        .processor(processor)
        .writer(writer)
        .build();
  }

  @Bean
  @StepScope
  public ItemReader<Integer> reader(
      @Value("#{jobParameters['count']}") Long count) {

    return new ListItemReader<>(
        IntStream.rangeClosed(1, count.intValue())
            .boxed()
            .toList()
    );
  }

  @Bean
  public JdbcBatchItemWriter<CustomerRow> writer() {

    JdbcBatchItemWriter<CustomerRow> writer =
        new JdbcBatchItemWriter<>();

    writer.setDataSource(dataSource);
    writer.setSql("""
        INSERT INTO customers
        (identification_num, name, customer_type, gender, birth_date,
         grade_code, preferred_contact, email, phone, created_at)
        VALUES
        (:identificationNum, :name, :customerType, :gender, :birthDate,
         :gradeCode, :preferredContact, :email, :phone, :createdAt)
    """);

    writer.setItemSqlParameterSourceProvider(
        new BeanPropertyItemSqlParameterSourceProvider<>());

    return writer;
  }
}