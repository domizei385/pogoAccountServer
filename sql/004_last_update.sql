ALTER TABLE accounts
    ADD last_updated BIGINT DEFAULT 0,
    ADD last_reason VARCHAR(50) AFTER last_returned;