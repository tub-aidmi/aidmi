{{ config(materialized='table') }}

WITH opportunity_raw AS (
    SELECT 
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),
account_clean AS (
    SELECT 
        UPPER(TRIM(REGEXP_REPLACE(id, '^[^A-Za-z0-9]+', ''))) AS clean_account_key,
        id AS "Id"
    FROM {{ source('fixture_messy_data_v2_src', 'account') }}
)
SELECT 
    -- Id: transformed key (strip prefix, uppercase)
    UPPER(TRIM(REGEXP_REPLACE(o.id, '^[^A-Za-z0-9]+', ''))) AS "Id",
    
    -- Name: INITCAP with TRIM and safe default
    INITCAP(TRIM(COALESCE(o.name, 'Unnamed Opportunity'))) AS "Name",
    
    -- StageName: map to allowed enum values; fallback to 'Prospecting' (NOT NULL)
    CASE 
        WHEN LOWER(TRIM(o.stagename)) IN (
            'prospecting', 'qualification', 'needs analysis', 'value proposition',
            'id. decision makers', 'perception analysis', 'proposal/price quote',
            'negotiation/review', 'closed won', 'closed lost'
        ) THEN INITCAP(TRIM(o.stagename))
        ELSE 'Prospecting'
    END AS "StageName",
    
    -- CloseDate: multi-format parser (DD.MM.YYYY → YYYYMMDD → ISO); NULL if invalid
    CASE 
        WHEN o.closedate IS NULL OR TRIM(o.closedate) = '' THEN NULL
        WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' 
            THEN TO_DATE(TRIM(o.closedate), 'DD.MM.YYYY')::TEXT
        WHEN o.closedate ~ '^\d{8}$' 
            THEN SUBSTR(TRIM(o.closedate), 1, 4) || '-' || 
                 SUBSTR(TRIM(o.closedate), 5, 2) || '-' || 
                 SUBSTR(TRIM(o.closedate), 7, 2)
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' 
            THEN TRIM(o.closedate)
        ELSE NULL
    END AS "CloseDate",
    
    -- Amount: strip symbols/prefixes; handle European dot-comma (1.234,56 → 1234.56)
    CASE 
        WHEN o.amount IS NULL OR TRIM(o.amount) = '' THEN NULL
        WHEN REGEXP_LIKE(TRIM(o.amount), '[0-9]\.[0-9]+,[0-9]+')  -- European: dot before comma
            THEN CAST(
                    REGEXP_REPLACE(
                        REPLACE(REPLACE(TRIM(o.amount), '$', ''), '€', ''),
                        '[.,]', 
                        CASE WHEN position(',' in REPLACE(TRIM(o.amount), '$', '')) > position('.' in REPLACE(TRIM(o.amount), '$', '')) THEN ',' ELSE '.' END,
                        'g'  -- replace all separators to single decimal point
                    ) AS DOUBLE PRECISION
                 )
        ELSE CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(TRIM(o.amount), '^\D+', ''), 
                    '[\$\€£]', '', 'g'
                )::DOUBLE PRECISION
            )
    END AS "Amount",
    
    -- CurrencyIsoCode: uppercase, trim; empty defaults to ''
    UPPER(TRIM(COALESCE(o.currencyisocode, ''))) AS "CurrencyIsoCode",
    
    -- AccountId: resolve via clean_key join to account table's canonical Id
    COALESCE(ac."Id", NULL) AS "AccountId",
    
    -- Legacy_Opportunity_ID__c: raw source id (unmodified) for auditability
    o.id AS "Legacy_Opportunity_ID__c",
    
    -- Derived date fields (not in source)
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    
    -- IsDeleted flag: default 0
    0 AS "IsDeleted"
FROM opportunity_raw o
LEFT JOIN account_clean ac 
    ON UPPER(TRIM(REGEXP_REPLACE(o.accountid, '^[^A-Za-z0-9]+', ''))) = ac.clean_account_key