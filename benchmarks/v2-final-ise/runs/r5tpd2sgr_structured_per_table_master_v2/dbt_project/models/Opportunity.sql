{{ config(materialized='table') }}

SELECT
    opp.opp_kennung AS "Id",
    opp.titel AS "Name",
    CASE
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('PROSPECTING') THEN 'Prospecting'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('QUALIFICATION') THEN 'Qualification'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('NEEDS ANALYSIS', 'BEDARFSANALYSE') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('VALUE PROPOSITION', 'WERTVORSCHLAG') THEN 'Value Proposition'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('ID. DECISION MAKERS', 'ENTSCHEIDUNGSTRÄGER') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('PERCEPTION ANALYSIS', 'WAHRNEHMUNGSANALYSE') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('PROPOSAL/PRICE QUOTE', 'ANGEBOT/PREISANGEBOT') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('NEGOTIATION/REVIEW', 'VERHANDLUNG/PRÜFUNG') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('CLOSED WON', 'GESCHLOSSEN GEWONNEN') THEN 'Closed Won'
        WHEN UPPER(TRIM(opp.vertriebsphase)) IN ('CLOSED LOST', 'GESCHLOSSEN VERLOREN') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN opp.zieldatum
        WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN opp.auftragswert ~ '^[0-9]+,[0-9]+$' THEN
            CAST(REPLACE(REPLACE(opp.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN opp.auftragswert ~ '^[0-9]+[.,]?[0-9]*$' THEN
            CAST(REPLACE(opp.auftragswert, ',', '.') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    UPPER(TRIM(opp.waehrungscode)) AS "CurrencyIsoCode",
    kund.kundennummer AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kund
    ON opp.kunden_ref = kund.kundennummer