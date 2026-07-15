{{ config(materialized='table') }}

SELECT 
    o."opp_kennung" AS "Id",
    o."titel" AS "Name",
    CASE 
        WHEN UPPER(TRIM(o."vertriebsphase")) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN UPPER(TRIM(o."vertriebsphase")) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION') THEN 'Qualification'
        WHEN UPPER(TRIM(o."vertriebsphase")) IN ('IN PRÜFUNG') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o."vertriebsphase")) IN ('IN KONTAKT') THEN 'Value Proposition'
        WHEN UPPER(TRIM(o."vertriebsphase")) IN ('WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)', 'CLOSED WON') THEN 'Closed Won'
        WHEN UPPER(TRIM(o."vertriebsphase")) IN ('LOST', 'VERLOREN', 'ABGESCHLOSSEN (VERLOREN)', 'CLOSED LOST') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN o."zieldatum" ~ '^\d{4}-\d{2}-\d{2}$' THEN o."zieldatum"
        WHEN o."zieldatum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o."zieldatum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o."zieldatum" ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o."zieldatum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o."zieldatum" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o."zieldatum", 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN o."auftragswert" IS NULL OR TRIM(o."auftragswert") = 'None' THEN NULL
        WHEN o."auftragswert" ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN 
            CAST(REPLACE(REPLACE(o."auftragswert", '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN o."auftragswert" ~ '^[0-9]+,[0-9]{2}$' THEN 
            CAST(REPLACE(o."auftragswert", ',', '.') AS DOUBLE PRECISION)
        WHEN o."auftragswert" ~ '^[0-9]+\.[0-9]{2}$' THEN 
            CAST(o."auftragswert" AS DOUBLE PRECISION)
        WHEN o."auftragswert" ~ '^[0-9]+$' THEN 
            CAST(o."auftragswert" AS DOUBLE PRECISION)
        WHEN o."auftragswert" ~ '^-[0-9]+\.[0-9]{2}$' THEN 
            CAST(o."auftragswert" AS DOUBLE PRECISION)
        ELSE 
            CAST(REGEXP_REPLACE(REGEXP_REPLACE(o."auftragswert", '[^0-9.-]', '', 'g'), '^([0-9]+)\.([0-9]{3}),([0-9]{2})$', '\1\2.\3', 'g') AS DOUBLE PRECISION)
    END AS "Amount",
    CASE 
        WHEN o."waehrungscode" IS NULL THEN NULL
        WHEN UPPER(TRIM(o."waehrungscode")) IN ('EUR', '€', 'EURO') THEN 'EUR'
        WHEN UPPER(TRIM(o."waehrungscode")) IN ('USD', '$', 'DOLLAR') THEN 'USD'
        WHEN UPPER(TRIM(o."waehrungscode")) IN ('CHF', 'GBP') THEN UPPER(TRIM(o."waehrungscode"))
        ELSE UPPER(TRIM(o."waehrungscode"))
    END AS "CurrencyIsoCode",
    k."kundennummer" AS "AccountId",
    o."opp_kennung" AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
    ON REPLACE(o."kunden_ref", 'KD-', 'CUST-') = k."kundennummer"