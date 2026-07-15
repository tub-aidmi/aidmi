{{ config(materialized='table') }}

WITH opportunity_data AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
),

account_mapping AS (
    SELECT
        kundennummer AS "AccountId",
        kundennummer AS "Legacy_Customer_ID__c"
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
    o.opp_kennung AS "Id",
    o.titel AS "Name",
    CASE
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PROSPECTING', 'PROSPECT', 'IN KONTAKT') THEN 'Prospecting'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION', 'IN PRÜFUNG') THEN 'Qualification'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('ID. DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSAL', 'PRICE QUOTE') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('NEGOTIATION/REVIEW', 'NEGOTIATION', 'REVIEW') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('CLOSED WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)', 'WON') THEN 'Closed Won'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('CLOSED LOST', 'VERLOREN', 'ABGESCHLOSSEN (VERLOREN)', 'LOST') THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN o.zieldatum
        WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.zieldatum = '0000-00-00' THEN NULL
        WHEN o.zieldatum IS NULL THEN NULL
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN o.auftragswert = 'None' OR o.auftragswert IS NULL OR TRIM(o.auftragswert) = '' THEN NULL
        WHEN o.auftragswert ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN
            CAST(REPLACE(REPLACE(o.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[0-9]+,[0-9]{2}$' THEN
            CAST(REPLACE(o.auftragswert, ',', '.') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[A-Za-z€$]+[0-9]+\.[0-9]{2}$' THEN
            CAST(REGEXP_REPLACE(o.auftragswert, '[^0-9.]', '', 'g') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[A-Za-z]+ [0-9]+\.[0-9]{2}$' THEN
            CAST(REGEXP_REPLACE(o.auftragswert, '[^0-9.]', '', 'g') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^-?[0-9]+\.[0-9]{2}$' THEN
            CAST(o.auftragswert AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^-?[0-9]+$' THEN
            CAST(o.auftragswert AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('EUR', 'EURO', '€') THEN 'EUR'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('USD', 'DOLLAR', '$') THEN 'USD'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('CHF', 'SWISS FRANC') THEN 'CHF'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('GBP', 'POUND') THEN 'GBP'
        ELSE UPPER(TRIM(o.waehrungscode))
    END AS "CurrencyIsoCode",
    am."AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_data o
LEFT JOIN account_mapping am ON REPLACE(o.kunden_ref, 'KD-M', 'CUST-M') = am."Legacy_Customer_ID__c"
