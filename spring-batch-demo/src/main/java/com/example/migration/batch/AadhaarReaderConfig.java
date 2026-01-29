package com.example.migration.batch;
import com.example.migration.model.AadhaarRecord;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.support.OraclePagingQueryProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;

import javax.sql.DataSource;
import java.util.Collections;

@Configuration
public class AadhaarReaderConfig {

  @Bean
  @StepScope
  public JdbcPagingItemReader<AadhaarRecord> reader(
      DataSource ds,
      @Value("#{stepExecutionContext['partitionIndex']}") Integer partitionIndex,
      @Value("#{stepExecutionContext['gridSize']}") Integer gridSize) {

    JdbcPagingItemReader<AadhaarRecord> r=new JdbcPagingItemReader<>();
    r.setDataSource(ds);
    r.setPageSize(500);

    OraclePagingQueryProvider qp=new OraclePagingQueryProvider();
    qp.setSelectClause("ROWID AS ROW_ID, ENCRYPTED_AADHAAR");
    qp.setFromClause("FROM DVS_ID_INFO");
    qp.setWhereClause(
      "WHERE MIG_STATUS = 'P' " +
      "AND MOD(ORA_HASH(ROWID), " + gridSize + ") = " + partitionIndex
    );
    qp.setSortKeys(Collections.singletonMap("ROWID", org.springframework.batch.item.database.Order.ASCENDING));

    r.setQueryProvider(qp);
    r.setRowMapper((rs,i)->{
      AadhaarRecord a=new AadhaarRecord();
      a.setRowId(rs.getString("ROW_ID"));
      a.setEncryptedAadhaar(rs.getBytes("ENCRYPTED_AADHAAR"));
      return a;
    });
    return r;
  }
}
