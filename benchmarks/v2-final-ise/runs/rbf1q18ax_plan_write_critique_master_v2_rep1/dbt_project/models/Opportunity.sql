{{ config(materialized='table') }}
SELECT
    opp.opp_kennung AS "Id",
    TRIM(opp.titel) AS "Name",
    CASE
        WHEN LOWER(TRIM(opp.vertriebsphase)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(opp.vertriebsphase)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('value proposition', 'wertangebot') THEN 'Value Proposition'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('id. decision makers', 'entscheidungsträger') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('proposal/price quote', 'angebot/preisangebot') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('negotiation/review', 'verhandlung/prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('closed won', 'abgeschlossen gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('closed lost', 'abgeschlossen verloren') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN opp.zieldatum ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN opp.auftragswert ~ '^[0-9]+(?:\.[0-9]{3})*(?:,[0-9]+)?$' THEN
            CAST(REGEXP_REPLACE(REGEXP_REPLACE(opp.auftragswert, '\.', '', 'g'), ',', '.', 'g') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    TRIM(opp.waehrungscode) AS "CurrencyIsoCode",
    kund.kundennummer AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} AS kund
    ON opp.kunden_ref = kund.kundennummer