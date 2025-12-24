-- Create rtbi schema and VERINT_TEXT_ANALYSIS table
-- Run this as: sqlplus sys/2288@localhost:1521/XE as sysdba @create_verint_table.sql

-- Create rtbi user if it doesn't exist
DECLARE
    user_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = 'RTBI';

    IF user_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE USER rtbi IDENTIFIED BY rtbi2024';
        EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE VIEW TO rtbi';
        EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO rtbi';
        DBMS_OUTPUT.PUT_LINE('✅ Created rtbi user');
    ELSE
        DBMS_OUTPUT.PUT_LINE('ℹ️  User rtbi already exists');
    END IF;
END;
/

-- Grant call_analytics access to rtbi schema
GRANT SELECT ON rtbi.VERINT_TEXT_ANALYSIS TO call_analytics;

-- Connect as rtbi to create table
CONNECT rtbi/rtbi2024@localhost:1521/XE;

-- Create VERINT_TEXT_ANALYSIS table
CREATE TABLE VERINT_TEXT_ANALYSIS (
    CALL_ID NUMBER NOT NULL,
    BAN VARCHAR2(50) NOT NULL,
    SUBSCRIBER_NO VARCHAR2(50),
    OWNER CHAR(1) NOT NULL, -- 'C' for Customer, 'A' for Agent
    TEXT CLOB NOT NULL,     -- Hebrew conversation text
    TEXT_TIME TIMESTAMP NOT NULL,   -- Message timestamp
    CALL_TIME TIMESTAMP NOT NULL,   -- Call start time
    CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Create indexes for CDC polling performance
CREATE INDEX IDX_VERINT_TEXT_TIME ON VERINT_TEXT_ANALYSIS (TEXT_TIME);
CREATE INDEX IDX_VERINT_CALL_ID ON VERINT_TEXT_ANALYSIS (CALL_ID, TEXT_TIME);

-- Insert sample test conversation (2 messages: customer + agent)
INSERT INTO VERINT_TEXT_ANALYSIS (CALL_ID, BAN, SUBSCRIBER_NO, OWNER, TEXT, TEXT_TIME, CALL_TIME)
VALUES (
    1001,
    '123456789',
    '0501234567',
    'C',
    'שלום, אני צריך עזרה עם החשבון שלי. הבעיה היא שאני לא מצליח להתחבר למערכת.',
    SYSTIMESTAMP,
    SYSTIMESTAMP - INTERVAL '5' SECOND
);

INSERT INTO VERINT_TEXT_ANALYSIS (CALL_ID, BAN, SUBSCRIBER_NO, OWNER, TEXT, TEXT_TIME, CALL_TIME)
VALUES (
    1001,
    '123456789',
    '0501234567',
    'A',
    'שלום, אני כאן לעזור. בוא ננסה לאפס את הסיסמה שלך. אתה יכול לאשר את המספר טלפון?',
    SYSTIMESTAMP + INTERVAL '3' SECOND,
    SYSTIMESTAMP - INTERVAL '5' SECOND
);

COMMIT;

-- Display summary
SELECT 'Table created successfully' AS status FROM dual;
SELECT COUNT(*) AS total_rows FROM VERINT_TEXT_ANALYSIS;
SELECT COUNT(DISTINCT CALL_ID) AS total_calls FROM VERINT_TEXT_ANALYSIS;

EXIT;
