{{ config(materialized='table') }}
SELECT
    MD5(o.opp_kennung) AS "Id",
    o.titel AS "Name",
    CASE
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('prospecting', 'prospect', 'in kontakt', 'in prüfung') THEN 'Prospecting'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('needs analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('value proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('id. decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('negotiation/review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('closed won', 'abgeschlossen (gewonnen)', 'gewonnen', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('closed lost', 'abgeschlossen (verloren)', 'verloren', 'lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN o.zieldatum
        WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN o.auftragswert IS NULL OR TRIM(o.auftragswert) IN ('None', '0') THEN NULL
        WHEN o.auftragswert ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN CAST(REPLACE(REPLACE(o.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[0-9]+,[0-9]+$' THEN CAST(REPLACE(o.auftragswert, ',', '.') AS DOUBLE PRECISION)
        ELSE CAST(REGEXP_REPLACE(o.auftragswert, '[^0-9.-]', '', 'g') AS DOUBLE PRECISION)
    END AS "Amount",
    CASE
        WHEN LOWER(TRIM(o.waehrungscode)) IN ('eur', 'euro', '€') THEN 'EUR'
        WHEN LOWER(TRIM(o.waehrungscode)) IN ('chf', 'chf.') THEN 'CHF'
        WHEN LOWER(TRIM(o.waehrungscode)) IN ('usd', '$', 'dollar') THEN 'USD'
        WHEN LOWER(TRIM(o.waehrungscode)) IN ('gbp', '£', 'pound') THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    MD5(k.kundennummer) AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} o
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON REPLACE(o.kunden_ref, 'KD-', 'CUST-') = k.kundennummer