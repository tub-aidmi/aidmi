{{ config(materialized='table') }}
SELECT 
    o.id AS "Id",
    COALESCE(INITCAP(TRIM(o.name)), 'Unknown') AS "Name",
    CASE 
        WHEN TRIM(o.stagename) IS NULL THEN NULL
        WHEN INITCAP(TRIM(o.stagename)) IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost') 
            THEN INITCAP(TRIM(o.stagename))
        ELSE NULL 
    END AS "StageName",
    CASE 
        WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL 
    END AS "CloseDate",
    CASE 
        WHEN o.amount ~ '^[0-9]+(\.[0-9]+)?$' THEN o.amount::DOUBLE PRECISION
        WHEN o.amount ~ '^[0-9]+,[0-9]+$' THEN REPLACE(o.amount, ',', '.')::DOUBLE PRECISION
        WHEN o.amount ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN REPLACE(REPLACE(o.amount, '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN o.amount ~ '^[^0-9]+([0-9]+(\.[0-9]+)?)$' THEN REGEXP_REPLACE(o.amount, '[^0-9.]', '', 'g')::DOUBLE PRECISION
        WHEN o.amount ~ '^[0-9]+[. ]+[0-9]+,[0-9]+$' THEN REPLACE(REGEXP_REPLACE(o.amount, '[. ]', '', 'g'), ',', '.')::DOUBLE PRECISION
        ELSE NULL 
    END AS "Amount",
    UPPER(TRIM(o.currencyisocode)) AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o