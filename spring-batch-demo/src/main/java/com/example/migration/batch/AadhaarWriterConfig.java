
package com.example.migration.batch;
import com.example.migration.model.AadhaarProcessedRecord;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import javax.sql.DataSource;
@Configuration
public class AadhaarWriterConfig {
  @Bean
  public JdbcBatchItemWriter<AadhaarProcessedRecord> writer(DataSource ds){
    JdbcBatchItemWriter<AadhaarProcessedRecord> w=new JdbcBatchItemWriter<>();
    w.setDataSource(ds);
    w.setSql("UPDATE AADHAAR_VAULT SET NEW_ENCRYPTED_AADHAAR=?, STATUS='C' WHERE ID=?");
    w.setItemPreparedStatementSetter((i,ps)->{
      ps.setBytes(1,i.getNewEncryptedAadhaar());
      ps.setString(2,i.getRowId());
    });
    return w;
  }
}
