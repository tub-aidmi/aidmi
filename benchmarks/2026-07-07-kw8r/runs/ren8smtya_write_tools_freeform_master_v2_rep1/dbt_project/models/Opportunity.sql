{{ config(materialized='table') }}

WITH account_mapping AS (
    SELECT 
        kundennummer,
        'ACCT-' || LPAD(ROW_NUMBER() OVER (ORDER BY kundennummer)::TEXT, 6, '0') AS account_id
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),
parsed_opportunities AS (
    SELECT
        opp.opp_kennung,
        opp.titel,
        opp.vertriebsphase,
        opp.zieldatum,
        opp.auftragswert,
        opp.waehrungscode,
        REPLACE(opp.kunden_ref, 'KD-M', 'CUST-M') AS mapped_kunden_ref
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} opp
)

SELECT
    'OPP-' || LPAD(ROW_NUMBER() OVER (ORDER BY po.opp_kennung)::TEXT, 6, '0') AS "Id",
    po.titel AS "Name",
    CASE 
        WHEN UPPER(TRIM(po.vertriebsphase)) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN UPPER(TRIM(po.vertriebsphase)) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION') THEN 'Qualification'
        WHEN UPPER(TRIM(po.vertriebsphase)) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(po.vertriebsphase)) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(po.vertriebsphase)) IN ('ID. DECISION MAKERS', 'ID DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(po.vertriebsphase)) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(po.vertriebsphase)) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSAL') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(po.vertriebsphase)) IN ('NEGOTIATION/REVIEW', 'NEGOTIATION') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(po.vertriebsphase)) IN ('CLOSED WON', 'WON', 'ABGESCHLOSSEN (GEWONNEN)', 'GEWONNEN') THEN 'Closed Won'
        WHEN UPPER(TRIM(po.vertriebsphase)) IN ('CLOSED LOST', 'LOST', 'ABGESCHLOSSEN (VERLOREN)', 'VERLOREN') THEN 'Closed Lost'
        WHEN UPPER(TRIM(po.vertriebsphase)) IN ('IN KONTAKT', 'IN CONTACT') THEN 'Prospecting'
        WHEN UPPER(TRIM(po.vertriebsphase)) IN ('IN PRÜFUNG', 'IN REVIEW') THEN 'Qualification'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN po.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN po.zieldatum
        WHEN po.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(po.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN po.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(po.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN po.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(po.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN po.zieldatum = '0000-00-00' THEN NULL
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN po.auftragswert = 'None' OR po.auftragswert IS NULL OR TRIM(po.auftragswert) = '' THEN NULL
        WHEN po.auftragswert ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN 
            CAST(REPLACE(REPLACE(po.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN po.auftragswert ~ '^[0-9]+,[0-9]+$' THEN 
            CAST(REPLACE(po.auftragswert, ',', '.') AS DOUBLE PRECISION)
        WHEN po.auftragswert ~ '^[0-9]+\.[0-9]+$' THEN 
            CAST(po.auftragswert AS DOUBLE PRECISION)
        WHEN po.auftragswert ~ '^[0-9]+$' THEN 
            CAST(po.auftragswert AS DOUBLE PRECISION)
        WHEN po.auftragswert ~ '^-?[0-9]+\.[0-9]+$' THEN 
            CAST(po.auftragswert AS DOUBLE PRECISION)
        WHEN po.auftragswert ~ '^-?[0-9]+$' THEN 
            CAST(po.auftragswert AS DOUBLE PRECISION)
        WHEN po.auftragswert ~ '^EUR [0-9]+\.[0-9]+$' THEN 
            CAST(REGEXP_REPLACE(po.auftragswert, '^EUR ', '') AS DOUBLE PRECISION)
        WHEN po.auftragswert ~ '^EUR [0-9]+,[0-9]+$' THEN 
            CAST(REPLACE(REGEXP_REPLACE(po.auftragswert, '^EUR ', ''), ',', '.') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE 
        WHEN UPPER(TRIM(po.waehrungscode)) IN ('EUR', 'CHF', 'USD', '$') THEN UPPER(TRIM(po.waehrungscode))
        ELSE NULL
    END AS "CurrencyIsoCode",
    am.account_id AS "AccountId",
    po.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM parsed_opportunities po
LEFT JOIN account_mapping am ON po.mapped_kunden_ref = am.kundennummer
