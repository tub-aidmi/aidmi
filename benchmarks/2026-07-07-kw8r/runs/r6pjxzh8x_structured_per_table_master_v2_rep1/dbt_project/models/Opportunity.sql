{{ config(materialized='table') }}

SELECT
    opp.opp_kennung AS "Id",
    COALESCE(NULLIF(TRIM(opp.titel), ''), 'Untitled Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('PROSPEKTIERUNG', 'PROSPEKTION') THEN 'Prospecting'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('QUALIFIKATION', 'QUALIFICATION') THEN 'Qualification'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('BEDARFSANALYSE', 'BEDARFS ANALYSE') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('WERTANGEBOT', 'WERT ANGEBOT') THEN 'Value Proposition'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('ENTSCHEIDUNGSFINDER', 'ENTSCHEIDUNGSTRÄGER') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('WAHRNEHMUNGSANALYSE', 'WAHRNEHMUNG ANALYSE') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('ANGEBOT', 'ANGEBOT/PREISANGEBOT') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('VERHANDLUNG', 'VERHANDLUNG/PRÜFUNG') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('Gewonnen', 'GEWONNEN') THEN 'Closed Won'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('VERLOREN', 'VERLOREN') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
            TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN 
            opp.zieldatum
        WHEN opp.zieldatum ~ '^\d{8}$' THEN 
            TO_CHAR(TO_DATE(opp.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN opp.auftragswert ~ '^[\d\s.]+,[\d]+$' THEN 
            CAST(REGEXP_REPLACE(REGEXP_REPLACE(opp.auftragswert, '[^\d,.]', '', 'g'), '\.', '', 'g') AS DOUBLE PRECISION) / 100
        WHEN opp.auftragswert ~ '^[\d\s]+\.[\d]+$' THEN 
            CAST(REGEXP_REPLACE(opp.auftragswert, '[^\d.]', '', 'g') AS DOUBLE PRECISION)
        WHEN opp.auftragswert ~ '^[\d]+$' THEN 
            CAST(opp.auftragswert AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    NULLIF(TRIM(opp.waehrungscode), '') AS "CurrencyIsoCode",
    k.kundennummer AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} opp
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
    ON opp.kunden_ref = k.kundennummer