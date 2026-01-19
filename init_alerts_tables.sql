-- ================================================
-- ALERT SYSTEM TABLES
-- Oracle schema for threshold-based alerting
-- ================================================

-- ALERT_CONFIGURATIONS: User-defined alert rules
CREATE TABLE ALERT_CONFIGURATIONS (
    ALERT_ID RAW(16) DEFAULT SYS_GUID() PRIMARY KEY,
    ALERT_NAME VARCHAR2(200) NOT NULL,
    ALERT_NAME_HE VARCHAR2(200),
    ALERT_TYPE VARCHAR2(50) DEFAULT 'threshold',    -- 'threshold', 'delta', 'trend'
    METRIC_SOURCE VARCHAR2(100) NOT NULL,            -- 'churn', 'sentiment', 'satisfaction', 'ml_quality', 'operational'
    METRIC_NAME VARCHAR2(100) NOT NULL,              -- 'high_risk_count', 'avg_score', 'negative_percent', etc.
    CONDITION_OPERATOR VARCHAR2(20) NOT NULL,        -- 'gt', 'lt', 'gte', 'lte', 'eq'
    THRESHOLD_VALUE NUMBER NOT NULL,
    TIME_WINDOW_HOURS NUMBER DEFAULT 24,
    FILTER_PRODUCT VARCHAR2(100),                    -- Optional: filter by product code (e.g., 'IZI')
    FILTER_SENTIMENT VARCHAR2(50),                   -- Optional: filter by sentiment
    SEVERITY VARCHAR2(20) DEFAULT 'WARNING',         -- 'INFO', 'WARNING', 'CRITICAL'
    IS_ENABLED NUMBER(1) DEFAULT 1,
    DESCRIPTION VARCHAR2(500),
    CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
    UPDATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
    CREATED_BY VARCHAR2(100)
);

-- ALERT_HISTORY: Triggered alerts log
CREATE TABLE ALERT_HISTORY (
    HISTORY_ID RAW(16) DEFAULT SYS_GUID() PRIMARY KEY,
    ALERT_ID RAW(16) NOT NULL,
    TRIGGERED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
    METRIC_VALUE NUMBER,
    THRESHOLD_VALUE NUMBER,
    SEVERITY VARCHAR2(20),
    STATUS VARCHAR2(20) DEFAULT 'ACTIVE',            -- 'ACTIVE', 'ACKNOWLEDGED', 'RESOLVED'
    ACKNOWLEDGED_BY VARCHAR2(100),
    ACKNOWLEDGED_AT TIMESTAMP,
    RESOLVED_BY VARCHAR2(100),
    RESOLVED_AT TIMESTAMP,
    RESOLUTION_NOTES VARCHAR2(1000),
    AFFECTED_SUBSCRIBERS CLOB,                       -- JSON array of subscriber data
    AFFECTED_COUNT NUMBER DEFAULT 0,
    CONSTRAINT FK_ALERT_HISTORY_CONFIG FOREIGN KEY (ALERT_ID)
        REFERENCES ALERT_CONFIGURATIONS(ALERT_ID) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IDX_ALERT_HISTORY_ALERT_ID ON ALERT_HISTORY(ALERT_ID);
CREATE INDEX IDX_ALERT_HISTORY_STATUS ON ALERT_HISTORY(STATUS);
CREATE INDEX IDX_ALERT_HISTORY_TRIGGERED ON ALERT_HISTORY(TRIGGERED_AT DESC);
CREATE INDEX IDX_ALERT_CONFIG_ENABLED ON ALERT_CONFIGURATIONS(IS_ENABLED);
CREATE INDEX IDX_ALERT_CONFIG_SOURCE ON ALERT_CONFIGURATIONS(METRIC_SOURCE);

-- ================================================
-- DEFAULT ALERT CONFIGURATIONS
-- Business-critical alerts for telecom analytics
-- ================================================

-- Alert 1: Daily High Churn Risk
-- Triggers when too many customers show churn intent in a day
INSERT INTO ALERT_CONFIGURATIONS (
    ALERT_NAME, ALERT_NAME_HE, ALERT_TYPE, METRIC_SOURCE, METRIC_NAME,
    CONDITION_OPERATOR, THRESHOLD_VALUE, TIME_WINDOW_HOURS,
    SEVERITY, DESCRIPTION
) VALUES (
    'Daily High Risk', 'סיכון גבוה יומי', 'threshold', 'churn', 'high_risk_count',
    'gt', 25, 24,
    'WARNING', 'More than 25 high-risk customers (70+) in 24 hours'
);

-- Alert 2: Critical Churn Surge
-- Immediate attention: customers very likely to churn
INSERT INTO ALERT_CONFIGURATIONS (
    ALERT_NAME, ALERT_NAME_HE, ALERT_TYPE, METRIC_SOURCE, METRIC_NAME,
    CONDITION_OPERATOR, THRESHOLD_VALUE, TIME_WINDOW_HOURS,
    SEVERITY, DESCRIPTION
) VALUES (
    'Critical Churn Surge', 'גל נטישה קריטי', 'threshold', 'churn', 'critical_risk_count',
    'gt', 10, 24,
    'CRITICAL', 'More than 10 critical-risk customers (90+) in 24 hours'
);

-- Alert 3: Negative Sentiment Wave
-- Service quality indicator
INSERT INTO ALERT_CONFIGURATIONS (
    ALERT_NAME, ALERT_NAME_HE, ALERT_TYPE, METRIC_SOURCE, METRIC_NAME,
    CONDITION_OPERATOR, THRESHOLD_VALUE, TIME_WINDOW_HOURS,
    SEVERITY, DESCRIPTION
) VALUES (
    'Negative Sentiment Wave', 'גל סנטימנט שלילי', 'threshold', 'sentiment', 'negative_percent',
    'gt', 35, 24,
    'WARNING', 'Negative sentiment exceeds 35% of daily calls'
);

-- Alert 4: IZI Product Alert
-- Product-specific monitoring for IZI customers
INSERT INTO ALERT_CONFIGURATIONS (
    ALERT_NAME, ALERT_NAME_HE, ALERT_TYPE, METRIC_SOURCE, METRIC_NAME,
    CONDITION_OPERATOR, THRESHOLD_VALUE, TIME_WINDOW_HOURS,
    FILTER_PRODUCT, SEVERITY, DESCRIPTION
) VALUES (
    'IZI Churn Risk', 'סיכון נטישה IZI', 'threshold', 'churn', 'high_risk_count',
    'gt', 15, 24,
    'IZI', 'WARNING', 'More than 15 IZI customers at high risk in 24 hours'
);

-- Alert 5: Low Satisfaction Drop
-- Customer experience indicator
INSERT INTO ALERT_CONFIGURATIONS (
    ALERT_NAME, ALERT_NAME_HE, ALERT_TYPE, METRIC_SOURCE, METRIC_NAME,
    CONDITION_OPERATOR, THRESHOLD_VALUE, TIME_WINDOW_HOURS,
    SEVERITY, DESCRIPTION
) VALUES (
    'Satisfaction Drop', 'ירידה בשביעות רצון', 'threshold', 'satisfaction', 'avg_satisfaction',
    'lt', 3.0, 24,
    'WARNING', 'Average satisfaction dropped below 3.0'
);

-- Alert 6: ML Backlog
-- Keep ML system maintained
INSERT INTO ALERT_CONFIGURATIONS (
    ALERT_NAME, ALERT_NAME_HE, ALERT_TYPE, METRIC_SOURCE, METRIC_NAME,
    CONDITION_OPERATOR, THRESHOLD_VALUE, TIME_WINDOW_HOURS,
    SEVERITY, DESCRIPTION
) VALUES (
    'ML Backlog', 'המלצות ממתינות', 'threshold', 'ml_quality', 'pending_count',
    'gte', 5, 168,
    'INFO', '5+ ML recommendations waiting for review'
);

COMMIT;

-- ================================================
-- HELPER VIEW FOR ACTIVE ALERTS WITH CONFIG DETAILS
-- ================================================

CREATE OR REPLACE VIEW V_ACTIVE_ALERTS AS
SELECT
    h.HISTORY_ID,
    h.ALERT_ID,
    c.ALERT_NAME,
    c.ALERT_NAME_HE,
    c.METRIC_SOURCE,
    c.METRIC_NAME,
    c.FILTER_PRODUCT,
    h.METRIC_VALUE,
    h.THRESHOLD_VALUE,
    h.SEVERITY,
    h.STATUS,
    h.TRIGGERED_AT,
    h.AFFECTED_COUNT,
    h.ACKNOWLEDGED_BY,
    h.ACKNOWLEDGED_AT
FROM ALERT_HISTORY h
JOIN ALERT_CONFIGURATIONS c ON h.ALERT_ID = c.ALERT_ID
WHERE h.STATUS = 'ACTIVE'
ORDER BY
    CASE h.SEVERITY
        WHEN 'CRITICAL' THEN 1
        WHEN 'WARNING' THEN 2
        ELSE 3
    END,
    h.TRIGGERED_AT DESC;
