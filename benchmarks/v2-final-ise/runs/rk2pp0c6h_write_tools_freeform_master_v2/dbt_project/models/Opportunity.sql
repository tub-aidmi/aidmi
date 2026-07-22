{{ config(materialized='table') }}

WITH opportunity_data AS (
    SELECT
        o.opp_kennung,
        o.titel,
        o.vertriebsphase,
        o.zieldatum,
        o.auftragswert,
        o.waehrungscode,
        o.kunden_ref,
        c.kundennummer AS account_kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c 
        ON REPLACE(o.kunden_ref, 'KD-M', 'CUST-M') = c.kundennummer
)

SELECT
    opp_kennung AS "Id",
    titel AS "Name",
    CASE 
        WHEN UPPER(TRIM(vertriebsphase)) IN ('PROSPECTING', 'IN KONTAKT') THEN 'Prospecting'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('QUALIFICATION', 'QUALI') THEN 'Qualification'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('NEEDS ANALYSIS', 'BEDARFSANALYSE') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('VALUE PROPOSITION', 'WERTVORSCHLAG') THEN 'Value Proposition'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('ID. DECISION MAKERS', 'ENTSCHEIDUNGSTRÄGER IDENTIFIZIEREN') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('PERCEPTION ANALYSIS', 'WAHRNEHMUNGSANALYSE') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('PROPOSAL/PRICE QUOTE', 'ANGEBOT') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('NEGOTIATION/REVIEW', 'VERHANDLUNG') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)') THEN 'Closed Won'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum
        WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN auftragswert IS NULL OR UPPER(TRIM(auftragswert)) = 'NONE' THEN NULL
        WHEN auftragswert ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN 
            CAST(REPLACE(REPLACE(auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN auftragswert ~ '^[0-9]+,[0-9]{2}$' THEN 
            CAST(REPLACE(auftragswert, ',', '.') AS DOUBLE PRECISION)
        WHEN auftragswert ~ '^[0-9]+\.[0-9]{2}$' THEN 
            CAST(auftragswert AS DOUBLE PRECISION)
        WHEN auftragswert ~ '^[A-Z]{3} [0-9]+\.[0-9]{2}$' THEN 
            CAST(REGEXP_REPLACE(auftragswert, '^[A-Z]{3} ', '') AS DOUBLE PRECISION)
        WHEN auftragswert ~ '^[A-Z]{3}[0-9]+\.[0-9]{2}$' THEN 
            CAST(REGEXP_REPLACE(auftragswert, '^[A-Z]{3}', '') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE 
        WHEN waehrungscode IS NOT NULL THEN UPPER(TRIM(waehrungscode))
        ELSE NULL
    END AS "CurrencyIsoCode",
    account_kundennummer AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_data
