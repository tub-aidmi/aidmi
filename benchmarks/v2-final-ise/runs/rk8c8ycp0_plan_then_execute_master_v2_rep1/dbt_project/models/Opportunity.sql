{{ config(materialized='table') }}

SELECT
    INITCAP(TRIM(mo.opp_kennung)) AS "Id",
    COALESCE(INITCAP(TRIM(mo.titel)), 'Unnamed Opportunity') AS "Name",
    CASE LOWER(INITCAP(TRIM(mo.vertriebsphase)))
        WHEN 'Prospecting' THEN 'Prospecting'
        WHEN 'Qualification' THEN 'Qualification'
        WHEN 'Needs Analysis' THEN 'Needs Analysis'
        WHEN 'Value Proposition' THEN 'Value Proposition'
        WHEN 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN 'Perception Analysis' THEN 'Perception Analysis'
        WHEN 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN 'Closed Won' THEN 'Closed Won'
        WHEN 'Closed Lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(mo.zieldatum) = '' OR mo.zieldatum IS NULL THEN NULL
        -- DD.MM.YYYY
        WHEN TRIM(mo.zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(mo.zieldatum), 'DD.MM.YYYY')::TEXT
        -- YYYYMMDD
        WHEN TRIM(mo.zieldatum) ~ '^\d{8}$' THEN
            SUBSTR(TRIM(mo.zieldatum), 1, 4) || '-' ||
            SUBSTR(TRIM(mo.zieldatum), 5, 2) || '-' ||
            SUBSTR(TRIM(mo.zieldatum), 7, 2)
        -- MM/DD/YYYY
        WHEN TRIM(mo.zieldatum) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(mo.zieldatum), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(mo.auftragswert) = '' OR mo.auftragswert IS NULL THEN NULL
        -- European format: dots as thousands separators, comma as decimal (e.g. 1.234,56)
        WHEN mo.auftragswert ~ '[,.].*[.,]' AND mo.auftragswert ~ '^[^,]*,[^,]*$'
            THEN REGEXP_REPLACE(REGEXP_REPLACE(TRIM(mo.auftragswert), '[\$\€£¥]', ''), '\.', '')::DOUBLE PRECISION / 100
        -- Plain number with dots as thousands (e.g. 1.234)
        WHEN mo.auftragswert ~ '^\d{1,3}(\.\d{3})+(\,\d+)?$' AND mo.auftragswert ~ '\,'
            THEN (REGEXP_REPLACE(TRIM(mo.auftragswert), '[\$\€£¥]', ''))::DOUBLE PRECISION
        -- Plain number with comma as decimal only (e.g. 1234,56)
        WHEN mo.auftragswert ~ '^\d+,\d+$'
            THEN REPLACE(TRIM(mo.auftragswert), ',', '.')::DOUBLE PRECISION
        -- Plain integer or decimal dot format
        ELSE TRIM(REGEXP_REPLACE(REPLACE(REPLACE(TRIM(mo.auftragswert), '$', ''), '€', ''), '£', ''))::DOUBLE PRECISION
    END AS "Amount",
    TRIM(mo.waehrungscode) AS "CurrencyIsoCode",
    INITCAP(TRIM(acc."Id")) AS "AccountId",
    INITCAP(TRIM(mo.opp_kennung)) AS "Legacy_Opportunity_ID__c",
    CAST(COALESCE(NULLIF(DATE_TRUNC('DAY', NOW())::TEXT, ''), '1900-01-01') AS TEXT) AS "CreatedDate",
    CAST(COALESCE(NULLIF(DATE_TRUNC('DAY', NOW())::TEXT, ''), '1900-01-01') AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
    ON INITCAP(TRIM(mk.kundennummer)) = INITCAP(TRIM(mo.kunden_ref))
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} acc_dummy  -- placeholder alias; see below
    ON 1 = 0