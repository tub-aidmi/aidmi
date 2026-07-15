{{ config(materialized='table') }}

SELECT
    MD5(opp_kennung) AS "Id",
    COALESCE(NULLIF(TRIM(titel), ''), 'Untitled Opportunity') AS "Name",
    CASE 
        WHEN LOWER(TRIM(vertriebsphase)) IN ('prospecting', 'prospektierung') THEN 'Prospecting'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('qualification', 'qualifizierung') THEN 'Qualification'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('value proposition', 'wertargumentation') THEN 'Value Proposition'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('id. decision makers', 'entscheider identifizieren') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('proposal/price quote', 'angebot/preisangebot') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('negotiation/review', 'verhandlung/prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('closed won', 'abgeschlossen gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('closed lost', 'abgeschlossen verloren') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN auftragswert ~ '^[\d]+[.,\d]+$' THEN 
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(auftragswert, '\.', '', 'g'),
                    ',', '.', 'g'
                )
                AS DOUBLE PRECISION
            )
        ELSE NULL
    END AS "Amount",
    NULLIF(TRIM(waehrungscode), '') AS "CurrencyIsoCode",
    CASE 
        WHEN kunden_ref IS NOT NULL THEN MD5(kunden_ref)
        ELSE NULL
    END AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
