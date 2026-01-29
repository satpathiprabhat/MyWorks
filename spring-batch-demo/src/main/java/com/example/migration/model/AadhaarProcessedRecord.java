
package com.example.migration.model;
public class AadhaarProcessedRecord {
  private String rowId;
  private byte[] newEncryptedAadhaar;
  public String getRowId(){return rowId;}
  public void setRowId(String rowId){this.rowId = rowId;}
  public byte[] getNewEncryptedAadhaar(){return newEncryptedAadhaar;}
  public void setNewEncryptedAadhaar(byte[] newEncryptedAadhaar){this.newEncryptedAadhaar=newEncryptedAadhaar;}
}
