{{ config(materialized='table') }}

SELECT
    '006' || SUBSTRING(MD5(o.opp_kennung) FROM 1 FOR 15) AS "Id",
    COALESCE(NULLIF(TRIM(o.titel), ''), 'Untitled Opportunity') AS "Name",
    CASE 
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('IN KONTAKT') THEN 'Prospecting'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('QUALIFICATION', 'QUALI') THEN 'Qualification'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('ID. DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PROPOSAL/PRICE QUOTE') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('NEGOTIATION/REVIEW') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)') THEN 'Closed Won'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)', 'LOST') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN o.zieldatum
        WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.zieldatum IS NULL THEN NULL
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN o.auftragswert IS NULL OR UPPER(TRIM(o.auftragswert)) = 'NONE' THEN NULL
        WHEN o.auftragswert ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN 
            CAST(REPLACE(REPLACE(o.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[0-9]+,[0-9]{2}$' THEN 
            CAST(REPLACE(o.auftragswert, ',', '.') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[0-9]+\.[0-9]{2}$' THEN 
            CAST(o.auftragswert AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[0-9]+$' THEN 
            CAST(o.auftragswert AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^-' THEN 
            CASE 
                WHEN o.auftragswert ~ '^- [0-9]+\.[0-9]{2}$' THEN CAST(REPLACE(o.auftragswert, ' ', '') AS DOUBLE PRECISION)
                WHEN o.auftragswert ~ '^- [0-9]+$' THEN CAST(REPLACE(o.auftragswert, ' ', '') AS DOUBLE PRECISION)
                ELSE NULL
            END
        WHEN o.auftragswert ~ '^[A-Z]{3} [0-9]+\.[0-9]{2}$' THEN 
            CAST(REGEXP_REPLACE(o.auftragswert, '^[A-Z]{3} ', '') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE 
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('EUR', '€', 'EURO') THEN 'EUR'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('USD', '$', 'DOLLAR') THEN 'USD'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('CHF') THEN 'CHF'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('GBP') THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    '001' || SUBSTRING(MD5(c.kundennummer) FROM 1 FOR 15) AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')::TEXT AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c
    ON REPLACE(o.kunden_ref, 'KD-', 'CUST-') = c.kundennummer
