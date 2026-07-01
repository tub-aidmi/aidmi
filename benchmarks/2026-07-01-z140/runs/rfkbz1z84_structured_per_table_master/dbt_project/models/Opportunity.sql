{{ config(materialized='table') }}

SELECT
    opp.opp_kennung AS "Id",
    INITCAP(TRIM(opp.titel)) AS "Name",
    CASE
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('prospecting', 'prospect', 'prospecting ', 'prospect ') THEN 'Prospecting'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('needs analysis', 'in prüfung', 'in kontakt') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('value proposition', 'value proposiion') THEN 'Value Proposition'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('id. decision makers', 'identifying decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('proposal/price quote', 'angebot/preisanfrage', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('negotiation/review', 'verhandlung', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('gewonnen', 'won', 'closed won', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('verloren', 'lost', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN opp.zieldatum IS NULL OR TRIM(opp.zieldatum) = '' OR opp.zieldatum = 'N/A' OR opp.zieldatum = '0000-00-00' THEN NULL
        WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN opp.zieldatum
        WHEN opp.zieldatum ~ '^\d{8}$' THEN TO_DATE(opp.zieldatum, 'YYYYMMDD')::TEXT
        WHEN opp.zieldatum ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$' THEN TO_DATE(opp.zieldatum, 'DD.MM.YYYY')::TEXT
        WHEN opp.zieldatum ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN TO_DATE(opp.zieldatum, 'M/D/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN opp.auftragswert IS NULL OR TRIM(opp.auftragswert) = '' OR opp.auftragswert = 'None' THEN NULL
        WHEN opp.auftragswert ~ ',' THEN
            CAST(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(opp.auftragswert, '[^\d.,-]', '', 'g'), '\.', ''), ',', '.') AS DOUBLE PRECISION)
        ELSE
            CAST(REGEXP_REPLACE(opp.auftragswert, '[^\d.-]', '', 'g') AS DOUBLE PRECISION)
    END AS "Amount",
    CASE
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('eur', '€') THEN 'EUR'
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('usd', 'dollar', '$') THEN 'USD'
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('chf') THEN 'CHF'
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('gbp', '£') THEN 'GBP'
        ELSE opp.waehrungscode
    END AS "CurrencyIsoCode",
    cust.kundennummer AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_opportunities') }} opp
LEFT JOIN {{ source('fixture_master_src', 'master_kunden') }} cust
    ON REGEXP_REPLACE(opp.kunden_ref, '^KD-', 'CUST-') = cust.kundennummer
