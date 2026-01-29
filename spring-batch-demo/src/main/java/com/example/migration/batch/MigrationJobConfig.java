package com.example.migration.batch;
import com.example.migration.model.AadhaarProcessedRecord;
import com.example.migration.model.AadhaarRecord;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
@EnableBatchProcessing
public class MigrationJobConfig {

  @Bean
  public Job job(JobBuilderFactory jbf, Step masterStep){
    return jbf.get("aadhaarMigrationJob")
      .start(masterStep)
      .build();
  }

  @Bean
  public Step masterStep(StepBuilderFactory sbf, Step slaveStep, JdbcTemplate jdbcTemplate){
    return sbf.get("masterStep")
      .partitioner("slaveStep", new RowIdPartitioner(jdbcTemplate, 4))
      .step(slaveStep)
      .taskExecutor(taskExecutor())
      .build();
  }

  @Bean
  public Step slaveStep(StepBuilderFactory sbf,
    JdbcPagingItemReader<AadhaarRecord> reader,
    AadhaarItemProcessor processor,
    JdbcBatchItemWriter<AadhaarProcessedRecord> writer,
    AadhaarSkipListener skipListener){

    return sbf.get("slaveStep")
      .<AadhaarRecord,AadhaarProcessedRecord>chunk(500)
      .reader(reader)
      .processor(processor)
      .writer(writer)
      .listener(skipListener)
      .faultTolerant()
      .retry(Exception.class)
      .retryLimit(3)
      .skip(Exception.class)
      .skipLimit(1000)
      .build();
  }

  @Bean
  public ThreadPoolTaskExecutor taskExecutor(){
    ThreadPoolTaskExecutor t=new ThreadPoolTaskExecutor();
    t.setCorePoolSize(4);
    t.setMaxPoolSize(4);
    t.initialize();
    return t;
  }
}
