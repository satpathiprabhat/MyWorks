package com.example.migration.batch;
import com.example.migration.hsm.HsmClient;
import com.example.migration.model.AadhaarProcessedRecord;
import com.example.migration.model.AadhaarRecord;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;
import java.util.concurrent.Semaphore;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
@Component
public class AadhaarItemProcessor implements ItemProcessor<AadhaarRecord, AadhaarProcessedRecord> {
  @Value("${migration.hsm.max-concurrency}")
  private int maxConcurrency;

  private static Semaphore HSM_SEMAPHORE;

  private final HsmClient hsm;

  @PostConstruct
  public void initSemaphore() {
      HSM_SEMAPHORE = new Semaphore(maxConcurrency, true);
  }

  public AadhaarItemProcessor(HsmClient hsm){this.hsm=hsm;}
  public AadhaarProcessedRecord process(AadhaarRecord item) throws Exception {
    HSM_SEMAPHORE.acquire();
    try {
      AadhaarProcessedRecord out = new AadhaarProcessedRecord();
      out.setRowId(item.getRowId());
      String plain = hsm.decrypt(item.getEncryptedAadhaar());
      out.setNewEncryptedAadhaar(hsm.encrypt(plain));
      return out;
    } finally {
      HSM_SEMAPHORE.release();
    }
  }
}
