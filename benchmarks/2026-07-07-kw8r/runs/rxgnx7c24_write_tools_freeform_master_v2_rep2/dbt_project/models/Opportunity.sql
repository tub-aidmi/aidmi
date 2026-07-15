{{ config(materialized='table') }}

SELECT
    o.opp_kennung AS "Id",
    COALESCE(o.titel, 'Untitled Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PROSPECTING', 'IN KONTAKT') THEN 'Prospecting'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION') THEN 'Qualification'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('NEEDS ANALYSIS', 'BEDARFSANALYSE') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('VALUE PROPOSITION', 'WERTVORSTELLUNG') THEN 'Value Proposition'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('ID. DECISION MAKERS', 'ENTSCHEIDUNGSTRÄGER IDENTIFIZIEREN') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PERCEPTION ANALYSIS', 'WAHRNEHMUNGSANALYSE') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PROPOSAL/PRICE QUOTE', 'ANGEBOT/PREISANGEBOT') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('NEGOTIATION/REVIEW', 'VERHANDLUNG/PRÜFUNG') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)', 'CLOSED WON') THEN 'Closed Won'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)', 'LOST') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN o.zieldatum ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN o.zieldatum
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN o.auftragswert IS NULL OR o.auftragswert = 'None' THEN NULL
        WHEN o.auftragswert ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN 
            CAST(REPLACE(REPLACE(o.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[0-9]+,[0-9]{2}$' THEN 
            CAST(REPLACE(o.auftragswert, ',', '.') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[0-9]+\.[0-9]+$' THEN 
            CAST(o.auftragswert AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[0-9]+$' THEN 
            CAST(o.auftragswert AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^-?[0-9]+\.[0-9]+$' THEN 
            CAST(o.auftragswert AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[A-Z]{2,4} [0-9]+\.[0-9]+$' THEN 
            CAST(REGEXP_REPLACE(o.auftragswert, '^[A-Z]{2,4} ', '') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[A-Z]{2,4}[0-9]+\.[0-9]+$' THEN 
            CAST(REGEXP_REPLACE(o.auftragswert, '^[A-Z]{2,4}', '') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('EUR', 'EURO', '€') THEN 'EUR'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('USD', 'DOLLAR', '$') THEN 'USD'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('CHF', 'SWISS FRANC') THEN 'CHF'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('GBP', 'POUND') THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    mk.kundennummer AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
    ON REPLACE(o.kunden_ref, 'KD-M', 'CUST-M') = mk.kundennummer
