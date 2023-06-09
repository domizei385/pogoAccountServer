ALTER TABLE accounts
    ADD purpose VARCHAR(20);

ALTER TABLE accounts_history
    ADD purpose VARCHAR(20) AFTER device;