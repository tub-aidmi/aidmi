{{ config(materialized='table') }}

SELECT
    'OPP_' || mo.opp_kennung AS "Id",
    COALESCE(NULLIF(TRIM(mo.titel), ''), 'Untitled Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(mo.vertriebsphase)) IN ('PROSPECTING') THEN 'Prospecting'
        WHEN UPPER(TRIM(mo.vertriebsphase)) IN ('QUALIFICATION', 'QUALIFIZIERUNG') THEN 'Qualification'
        WHEN UPPER(TRIM(mo.vertriebsphase)) IN ('NEEDS ANALYSIS', 'BEDARFSANALYSE') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(mo.vertriebsphase)) IN ('VALUE PROPOSITION', 'WERTVORSCHLAG') THEN 'Value Proposition'
        WHEN UPPER(TRIM(mo.vertriebsphase)) IN ('ID. DECISION MAKERS', 'ENTSCHEIDUNGSTRÄGER IDENTIFIZIEREN') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(mo.vertriebsphase)) IN ('PERCEPTION ANALYSIS', 'WAHRNEHMUNGSANALYSE') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(mo.vertriebsphase)) IN ('PROPOSAL/PRICE QUOTE', 'ANGEBOT/PREISANGEBOT') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(mo.vertriebsphase)) IN ('NEGOTIATION/REVIEW', 'VERHANDLUNG/ÜBERPRÜFUNG') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(mo.vertriebsphase)) IN ('CLOSED WON', 'GESCHLOSSEN GEWONNEN') THEN 'Closed Won'
        WHEN UPPER(TRIM(mo.vertriebsphase)) IN ('CLOSED LOST', 'GESCHLOSSEN VERLOREN') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN mo.zieldatum
        WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mo.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN mo.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN mo.auftragswert ~ '^\d+,\d+$' THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(mo.auftragswert, '\.', '', 'g'), ',', '.', 'g') AS DOUBLE PRECISION)
        WHEN mo.auftragswert ~ '^\d+\.\d+$' THEN CAST(mo.auftragswert AS DOUBLE PRECISION)
        WHEN mo.auftragswert ~ '^\d+$' THEN CAST(mo.auftragswert AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    COALESCE(NULLIF(TRIM(UPPER(mo.waehrungscode)), ''), 'EUR') AS "CurrencyIsoCode",
    'ACC_' || mk.kundennummer AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk ON mo.kunden_ref = mk.kundennummer