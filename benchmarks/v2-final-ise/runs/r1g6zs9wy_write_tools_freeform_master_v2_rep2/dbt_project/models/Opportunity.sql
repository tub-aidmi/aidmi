{{ config(materialized='table') }}

SELECT 
    MD5(opp_kennung) AS "Id",
    TRIM(titel) AS "Name",
    CASE 
        WHEN LOWER(TRIM(vertriebsphase)) IN ('prospektion', 'prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('qualifikation', 'qualification') THEN 'Qualification'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('bedarfsanalyse', 'needs analysis', 'bedarf') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('wertversprechen', 'value proposition', 'wert') THEN 'Value Proposition'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('entscheidungsträger identifizieren', 'id. decision makers', 'entscheidungstraeger') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('wahrnehmungsanalyse', 'perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('angebot/preis', 'proposal/price quote', 'angebot') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('verhandlung/prüfung', 'negotiation/review', 'verhandlung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('gewonnen', 'closed won', 'abgeschlossen gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('verloren', 'closed lost', 'abgeschlossen verloren') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN zieldatum ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD-MM-YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN auftragswert ~ '^[\d.,]+$' THEN 
            CAST(REGEXP_REPLACE(REGEXP_REPLACE(auftragswert, '\.', ''), ',', '.') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    TRIM(waehrungscode) AS "CurrencyIsoCode",
    MD5(kunden_ref) AS "AccountId",
    TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}