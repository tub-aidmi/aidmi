{{ config(materialized='table') }}

SELECT
    "opp_kennung" AS "Id",
    TRIM("titel") AS "Name",
    CASE
        WHEN TRIM(LOWER("vertriebsphase")) IN ('prospektierung', 'prospecting') THEN 'Prospecting'
        WHEN TRIM(LOWER("vertriebsphase")) IN ('qualifizierung', 'qualification') THEN 'Qualification'
        WHEN TRIM(LOWER("vertriebsphase")) IN ('bedarfsanalyse', 'needs analysis') THEN 'Needs Analysis'
        WHEN TRIM(LOWER("vertriebsphase")) IN ('wertversprechen', 'value proposition') THEN 'Value Proposition'
        WHEN TRIM(LOWER("vertriebsphase")) IN ('entscheidungsträger identifizieren', 'id. decision makers') THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER("vertriebsphase")) IN ('wahrnehmungsanalyse', 'perception analysis') THEN 'Perception Analysis'
        WHEN TRIM(LOWER("vertriebsphase")) IN ('angebot/preisangebot', 'proposal/price quote') THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER("vertriebsphase")) IN ('verhandlung/überprüfung', 'negotiation/review') THEN 'Negotiation/Review'
        WHEN TRIM(LOWER("vertriebsphase")) IN ('abgeschlossen gewonnen', 'closed won') THEN 'Closed Won'
        WHEN TRIM(LOWER("vertriebsphase")) IN ('abgeschlossen verloren', 'closed lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM("zieldatum") ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE("zieldatum", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM("zieldatum") ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE("zieldatum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM("zieldatum") ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE("zieldatum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM("zieldatum") ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM("zieldatum")
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM("auftragswert") ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN 
            CAST(REGEXP_REPLACE(REGEXP_REPLACE("auftragswert", '\.', '', 'g'), ',', '.') AS DOUBLE PRECISION)
        WHEN TRIM("auftragswert") ~ '^[0-9]+,[0-9]+$' THEN 
            CAST(REGEXP_REPLACE("auftragswert", ',', '.') AS DOUBLE PRECISION)
        WHEN TRIM("auftragswert") ~ '^[0-9]+\.[0-9]+$' THEN 
            CAST("auftragswert" AS DOUBLE PRECISION)
        WHEN TRIM("auftragswert") ~ '^[0-9]+$' THEN 
            CAST("auftragswert" AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    TRIM("waehrungscode") AS "CurrencyIsoCode",
    COALESCE(
        (SELECT "kundennummer" FROM {{ source('fixture_master_v2_src', 'master_kunden') }} WHERE "kundennummer" = TRIM("kunden_ref")),
        (SELECT "kundennummer" FROM {{ source('fixture_master_v2_src', 'master_kunden') }} WHERE "kundennummer" = "kunden_ref")
    ) AS "AccountId",
    "opp_kennung" AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
