{{ config(materialized='table') }}

WITH account_key_map AS (
    SELECT
        UPPER(TRIM(REGEXP_REPLACE(id, '^[^A-Za-z0-9]+', ''))) AS clean_key,
        id AS "Id"
    FROM {{ source('fixture_messy_data_v2_src', 'account') }}
),

clean_opportunity AS (
    SELECT
        o.*,
        UPPER(TRIM(REGEXP_REPLACE(o.accountid, '^[^A-Za-z0-9]+', ''))) AS clean_account_key
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o
)

SELECT
    -- Id: clean source key (strip non-alphanumeric prefix, uppercase)
    UPPER(TRIM(REGEXP_REPLACE(co.id, '^[^A-Za-z0-9]+', ''))) AS "Id",

    -- Name: INITCAP with TRIM; default to "Unnamed Opportunity" if missing
    INITCAP(TRIM(COALESCE(co.name, 'Unnamed Opportunity'))) AS "Name",

    -- StageName: map source values into the declared enum domain (NOT NULL, fallback "Prospecting")
    CASE
        WHEN LOWER(TRIM(COALESCE(co.stagename, ''))) IN ('prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(COALESCE(co.stagename, ''))) IN ('qualification') THEN 'Qualification'
        WHEN LOWER(TRIM(COALESCE(co.stagename, ''))) IN ('needs analysis', 'needs_analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(COALESCE(co.stagename, ''))) IN ('value proposition', 'value_proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(COALESCE(co.stagename, ''))) LIKE '%decision maker%' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(COALESCE(co.stagename, ''))) IN ('perception analysis', 'perception_analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(COALESCE(co.stagename, ''))) LIKE '%proposal%' OR LOWER(TRIM(COALESCE(co.stagename, ''))) LIKE '%price quote%' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(COALESCE(co.stagename, ''))) LIKE '%negotiation%' OR LOWER(TRIM(COALESCE(co.stagename, ''))) LIKE '%review%' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(COALESCE(co.stagename, ''))) IN ('closed won', 'closed_won') THEN 'Closed Won'
        WHEN LOWER(TRIM(COALESCE(co.stagename, ''))) IN ('closed lost', 'closed_lost') THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",

    -- CloseDate: multi-format parser → ISO YYYY-MM-DD text; NULL if unparseable (NOT NULL target gets coalesced to current date fallback)
    CASE
        WHEN co.closedate IS NULL OR TRIM(co.closedate) = '' THEN CURRENT_DATE::TEXT
        WHEN co.closedate ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE(TRIM(co.closedate), 'DD.MM.YYYY')::TEXT
        WHEN co.closedate ~ '^\d{8}$'
            THEN SUBSTR(TRIM(co.closedate), 1, 4) || '-' ||
                 SUBSTR(TRIM(co.closedate), 5, 2) || '-' ||
                 SUBSTR(TRIM(co.closedate), 7, 2)
        WHEN co.closedate ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TRIM(co.closedate)
        ELSE CURRENT_DATE::TEXT
    END AS "CloseDate",

    -- Amount: strip currency symbols/text prefixes; handle European dot-comma format. Guard against empty strings to avoid double precision cast errors.
    CASE
        WHEN co.amount IS NULL OR TRIM(co.amount) = '' THEN NULL::DOUBLE PRECISION
        ELSE
            CASE
                -- European format detected: digits.digits,digits (e.g. "1.234,56")
                WHEN REGEXP_REPLACE(TRIM(co.amount), '[^\d.,]', '', 'g') ~ '^\d+\.\d{1,2},\d+$'
                    THEN REPLACE(REPLACE(TRIM(co.amount), '.', ''), ',', '.')::DOUBLE PRECISION
                -- Standard numeric: strip non-numeric characters (keep digits and dots only)
                ELSE CASE
                        WHEN REGEXP_REPLACE(TRIM(co.amount), '[^\d.]+', '', 'g') ~ '^\d+(\.\d+)?$'
                            THEN CAST(REGEXP_REPLACE(TRIM(co.amount), '[^\d.]+', '', 'g') AS DOUBLE PRECISION)
                        ELSE NULL::DOUBLE PRECISION
                     END
            END
    END AS "Amount",

    -- CurrencyIsoCode: uppercase, trim; empty string defaults to ''
    UPPER(TRIM(COALESCE(co.currencyisocode, ''))) AS "CurrencyIsoCode",

    -- AccountId: resolve via clean_key join to Salesforce-style Account Id
    COALESCE(ac."Id", NULL) AS "AccountId",

    -- Legacy_Opportunity_ID__c: raw source id for row-level auditability
    co.id AS "Legacy_Opportunity_ID__c",

    -- Derived date fields
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",

    -- IsDeleted flag: default 0
    0 AS "IsDeleted"

FROM clean_opportunity co
LEFT JOIN account_key_map ac
    ON co.clean_account_key = ac.clean_key