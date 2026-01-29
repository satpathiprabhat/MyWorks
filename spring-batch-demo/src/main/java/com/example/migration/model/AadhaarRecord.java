package com.example.migration.model;

public class AadhaarRecord {

    /**
     * Oracle ROWID for identifying the record uniquely during migration.
     * This is required because the table does not have a numeric ID column.
     */
    private String rowId;

    /**
     * Existing encrypted Aadhaar value (old cipher).
     */
    private byte[] encryptedAadhaar;

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    public byte[] getEncryptedAadhaar() {
        return encryptedAadhaar;
    }

    public void setEncryptedAadhaar(byte[] encryptedAadhaar) {
        this.encryptedAadhaar = encryptedAadhaar;
    }
}
