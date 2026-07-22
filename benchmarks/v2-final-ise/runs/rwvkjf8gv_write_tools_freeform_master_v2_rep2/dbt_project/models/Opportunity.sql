{{ config(materialized='table') }}

SELECT
    '006' || REPLACE(o.opp_kennung, 'OPP-', '') AS "Id",
    COALESCE(NULLIF(TRIM(o.titel), ''), 'Untitled Opportunity') AS "Name",
    CASE 
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PROSPECTING', 'IN KONTAKT', 'CONTACT') THEN 'Prospecting'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('QUALIFICATION', 'QUALI', 'QUALIFIZIERUNG') THEN 'Qualification'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('NEEDS ANALYSIS', 'BEDARFSANALYSE') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('VALUE PROPOSITION', 'WERTVORSCHLAG') THEN 'Value Proposition'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('ID. DECISION MAKERS', 'ENTSCHEIDUNGSTRÄGER IDENTIFIZIEREN') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PERCEPTION ANALYSIS', 'WAHRNEHMUNGSANALYSE') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PROPOSAL/PRICE QUOTE', 'ANGEBOT', 'ANGEBOTSLEGUNG') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('NEGOTIATION/REVIEW', 'VERHANDLUNG', 'PRÜFUNG') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)', 'GEWONNEN') THEN 'Closed Won'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)', 'VERLOREN') THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE 
        WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN o.zieldatum
        WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum = '0000-00-00' THEN NULL
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN o.auftragswert ~ '^\d+\.\d+$' THEN CAST(o.auftragswert AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[A-Za-z]+ [\d]+\.\d+$' THEN CAST(REGEXP_REPLACE(o.auftragswert, '[A-Za-z]+ ', '') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[A-Za-z]+[\d]+\.\d+$' THEN CAST(REGEXP_REPLACE(o.auftragswert, '[A-Za-z]+', '') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^\d+,\d+$' THEN CAST(REPLACE(o.auftragswert, ',', '.') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^\d+\.\d+,\d+$' THEN CAST(REPLACE(REPLACE(o.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^\$[\d]+\.\d+$' THEN CAST(REPLACE(o.auftragswert, '$', '') AS DOUBLE PRECISION)
        WHEN o.auftragswert = 'None' THEN NULL
        ELSE NULL
    END AS "Amount",
    CASE 
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('EUR', '€') THEN 'EUR'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('USD', '$') THEN 'USD'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('CHF', 'SFR') THEN 'CHF'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('GBP', '£') THEN 'GBP'
        ELSE NULLIF(TRIM(o.waehrungscode), '')
    END AS "CurrencyIsoCode",
    CASE 
        WHEN o.kunden_ref IS NOT NULL THEN '001' || REPLACE(o.kunden_ref, 'KD-M', '')
        ELSE NULL
    END AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o
