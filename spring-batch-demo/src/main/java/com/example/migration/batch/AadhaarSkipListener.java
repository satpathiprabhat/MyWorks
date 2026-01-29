package com.example.migration.batch;

import com.example.migration.dao.AadhaarStatusDao;
import com.example.migration.model.AadhaarProcessedRecord;
import com.example.migration.model.AadhaarRecord;
import org.springframework.batch.core.SkipListener;
import org.springframework.stereotype.Component;

@Component
public class AadhaarSkipListener implements SkipListener<AadhaarRecord, AadhaarProcessedRecord> {

    private final AadhaarStatusDao statusDao;

    public AadhaarSkipListener(AadhaarStatusDao statusDao) {
        this.statusDao = statusDao;
    }

    @Override
    public void onSkipInRead(Throwable t) {
        // No-op: no record identifier available at read time
    }

    @Override
    public void onSkipInProcess(AadhaarRecord item, Throwable t) {
        if (item != null && item.getRowId() != null) {
            statusDao.markErrorByRowId(item.getRowId(), buildErrorMessage(t));
        }
    }

    @Override
    public void onSkipInWrite(AadhaarProcessedRecord item, Throwable t) {
        if (item != null && item.getRowId() != null) {
            statusDao.markErrorByRowId(item.getRowId(), buildErrorMessage(t));
        }
    }

    private String buildErrorMessage(Throwable t) {
        if (t == null) {
            return "Unknown error";
        }
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }
}
