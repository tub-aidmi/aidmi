{{ config(materialized='table') }}
SELECT 
    MD5(o.opp_kennung) AS "Id",
    COALESCE(NULLIF(TRIM(INITCAP(o.titel)), ''), 'Untitled Opportunity') AS "Name",
    CASE 
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PROSPECTING', 'PROSPECT', 'IN KONTAKT') THEN 'Prospecting'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION', 'IN PRÜFUNG') THEN 'Qualification'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('ID. DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PROPOSAL/PRICE QUOTE') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('NEGOTIATION/REVIEW') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('CLOSED WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)', 'WON') THEN 'Closed Won'
        WHEN UPPER(TRIM(o.vertriebsphase)) IN ('CLOSED LOST', 'VERLOREN', 'ABGESCHLOSSEN (VERLOREN)', 'LOST') THEN 'Closed Lost'
        ELSE NULL 
    END AS "StageName",
    CASE 
        WHEN o.zieldatum ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL 
    END AS "CloseDate",
    CASE 
        WHEN o.auftragswert NOT IN ('None', 'EUR None') 
             AND o.auftragswert ~ '^[\d\s\-\.,EURCHFUSD€$]+$' 
        THEN 
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(o.auftragswert, '[^0-9\-,.]', '', 'g'),
                                '^\-?$', '0'
                            ),
                            '^\-?0+', '0'
                        ),
                        '\.', '', 'g'
                    ),
                    ',', '.', 'g'
                ) AS DOUBLE PRECISION
            )
        ELSE NULL 
    END AS "Amount",
    CASE 
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('EUR', 'EURO', '€') THEN 'EUR'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('CHF') THEN 'CHF'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('USD', 'DOLLAR', '$') THEN 'USD'
        WHEN UPPER(TRIM(o.waehrungscode)) IN ('GBP') THEN 'GBP'
        ELSE NULL 
    END AS "CurrencyIsoCode",
    MD5(k.kundennummer) AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
    ON REPLACE(o.kunden_ref, 'KD-', 'CUST-') = k.kundennummer