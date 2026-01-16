# Current Fixes

## 1. Subscriber Recognition Pattern

**The Pattern:**
```sql
-- CONVERSATION_SUMMARY stores SUBSCRIBER_NO as NUMBER
-- SUBSCRIBER table expects SUBSCRIBER_NO as VARCHAR
-- Use || ' ' to convert NUMBER to string for matching

-- In WHERE clause:
cs.SUBSCRIBER_NO || ' ' = :subscriber_no

-- In SELECT:
cs.SUBSCRIBER_NO || ' ' as subscriber_no

-- In JOIN:
LEFT JOIN SUBSCRIBER s
    ON s.SUBSCRIBER_NO = cs.SUBSCRIBER_NO || ' '
    AND s.CUSTOMER_BAN = cs.BAN
```

**Fixed in `routes/new_features.py` - Customer Journey:**
- Lines 368-376: WHERE conditions now use `SUBSCRIBER_NO || ' '`
- Line 390: SELECT now returns `SUBSCRIBER_NO || ' '` as string
- Lines 424-450: Subscriber status query now handles both cases properly

---

# 2. Fix Sentiment Query Mismatch

## The Problem

**Data Storage (CDC Service) uses NUMERIC 1-5 scale:**
```python
# From cdc_service_prod_fixed.py:952-1002
sentiment_map = {'חיובי': 4, 'positive': 4, 'שלילי': 2, 'negative': 2,
                 'neutral': 3, 'נייטרלי': 3}
sentiment = sentiment_map.get(sentiment_raw.lower().strip(), 3)
# Stored as: 'sentiment': sentiment  # Numeric 1-5
```

**But Route Queries treat SENTIMENT as TEXT:**
```sql
-- WRONG: Trying to match text patterns on a numeric column!
LOWER(SENTIMENT) LIKE '%חיובי%' OR LOWER(SENTIMENT) LIKE '%positive%'
```

This will **always fail** because SENTIMENT column contains numbers (1, 2, 3, 4, 5), not text strings.

---

## The Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    CONVERSATION_SUMMARY Table                    │
├─────────────────────────────────────────────────────────────────┤
│  SENTIMENT column: NUMBER(1)                                     │
│                                                                  │
│    1 = Very Negative                                             │
│    2 = Negative        ─────► "Negative" in UI                   │
│    3 = Neutral         ─────► "Neutral" in UI                    │
│    4 = Positive        ─────► "Positive" in UI                   │
│    5 = Very Positive                                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Files to Fix

### 1. `routes/new_features.py`

**Lines 112-115 (current period sentiment):**
```sql
-- WRONG:
COUNT(CASE WHEN LOWER(SENTIMENT) LIKE '%חיובי%' OR LOWER(SENTIMENT) LIKE '%positive%' THEN 1 END) as positive,
COUNT(CASE WHEN LOWER(SENTIMENT) LIKE '%שלילי%' OR LOWER(SENTIMENT) LIKE '%negative%' THEN 1 END) as negative,

-- CORRECT:
COUNT(CASE WHEN SENTIMENT >= 4 THEN 1 END) as positive,
COUNT(CASE WHEN SENTIMENT <= 2 THEN 1 END) as negative,
COUNT(CASE WHEN SENTIMENT = 3 OR SENTIMENT IS NULL THEN 1 END) as neutral,
```

**Lines 129-132 (previous period - same fix)**

### 2. `routes/analytics.py`

**Lines 23-25 (summary endpoint):**
```sql
-- WRONG:
COUNT(CASE WHEN LOWER(SENTIMENT) LIKE '%חיובי%' ... THEN 1 END) as positive,

-- CORRECT:
COUNT(CASE WHEN SENTIMENT >= 4 THEN 1 END) as positive,
COUNT(CASE WHEN SENTIMENT <= 2 THEN 1 END) as negative,
COUNT(CASE WHEN SENTIMENT = 3 OR SENTIMENT IS NULL THEN 1 END) as neutral
```

**Lines 61-72 (sentiment endpoint):**
```sql
-- WRONG:
CASE WHEN LOWER(SENTIMENT) LIKE '%חיובי%' ... THEN 'Positive' ...

-- CORRECT:
CASE
    WHEN SENTIMENT >= 4 THEN 'Positive'
    WHEN SENTIMENT <= 2 THEN 'Negative'
    ELSE 'Neutral'
END as sentiment,
```

### 3. `routes/calls.py`

**Lines 49-54 (sentiment_calls endpoint):**
```python
# WRONG:
if sentiment_type == 'Positive':
    sentiment_condition = "(LOWER(SENTIMENT) LIKE '%חיובי%' ...)"

# CORRECT:
if sentiment_type == 'Positive':
    sentiment_condition = "SENTIMENT >= 4"
elif sentiment_type == 'Negative':
    sentiment_condition = "SENTIMENT <= 2"
else:  # Neutral
    sentiment_condition = "(SENTIMENT = 3 OR SENTIMENT IS NULL)"
```

---

## Sentiment Scale Reference

| Numeric | Meaning        | UI Label   |
|---------|----------------|------------|
| 1       | Very Negative  | Negative   |
| 2       | Negative       | Negative   |
| 3       | Neutral        | Neutral    |
| 4       | Positive       | Positive   |
| 5       | Very Positive  | Positive   |

---

## Implementation Order

1. Fix `routes/new_features.py` (trend comparisons)
2. Fix `routes/analytics.py` (summary & sentiment charts)
3. Fix `routes/calls.py` (sentiment drill-down)

Each fix is isolated and can be tested independently.
