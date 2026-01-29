package com.example.migration.dao;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class AadhaarStatusDao {

    private final JdbcTemplate jdbcTemplate;

    public AadhaarStatusDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void markErrorByRowId(String rowId, String errorReason) {
        jdbcTemplate.update(
            "UPDATE AADHAAR_VAULT " +
            "SET STATUS = 'E', ERROR_REASON = ? " +
            "WHERE ROWID = ?",
            truncate(errorReason, 1000),
            rowId
        );
    }

    private String truncate(String msg, int maxLen) {
        if (msg == null) {
            return null;
        }
        return msg.length() <= maxLen ? msg : msg.substring(0, maxLen);
    }
}
