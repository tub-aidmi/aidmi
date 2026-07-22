{{ config(materialized='table') }}

SELECT
    '006' || SUBSTRING(MD5(opps.opp_kennung) FROM 1 FOR 15) AS "Id",
    COALESCE(INITCAP(TRIM(opps.titel)), 'Unnamed Opportunity') AS "Name",
    CASE LOWER(TRIM(opps.vertriebsphase))
        WHEN 'in kontakt' THEN 'Prospecting'
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'prospect' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'quali' THEN 'Qualification'
        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'in prüfung' THEN 'Needs Analysis'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
        WHEN 'won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'verloren' THEN 'Closed Lost'
        WHEN 'lost' THEN 'Closed Lost'
        WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN opps.zieldatum IS NULL OR TRIM(opps.zieldatum) = '' THEN NULL
        WHEN opps.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(opps.zieldatum, 'DD.MM.YYYY')::TEXT
        WHEN opps.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(opps.zieldatum)
        WHEN opps.zieldatum ~ '^\d{8}$' THEN TO_DATE(opps.zieldatum, 'YYYYMMDD')::TEXT
        WHEN opps.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(opps.zieldatum), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(opps.auftragswert) IS NULL OR TRIM(opps.auftragswert) = '' THEN NULL
        WHEN UPPER(TRIM(opps.auftragswert)) = 'NONE' THEN NULL
        -- European format: digits.digits,digits (e.g., "400.902,63") - dot as thousands sep, comma as decimal
        WHEN TRIM(opps.auftragswert) ~ '^\-?\d+\.\d{3},\d+$' THEN
            CAST(
                REGEXP_REPLACE(REPLACE(TRIM(opps.auftragswert), '.', ''), ',', '.') AS DOUBLE PRECISION
            )
        -- Standard format with or without prefix (e.g., "EUR 144893.69", "116121.28")
        ELSE
            CAST(
                REGEXP_REPLACE(TRIM(opps.auftragswert), '[^0-9.\-]', '', 'g') AS DOUBLE PRECISION
            )
    END AS "Amount",
    CASE UPPER(TRIM(opps.waehrungscode))
        WHEN '€' THEN 'EUR'
        WHEN '$' THEN 'USD'
        WHEN '£' THEN 'GBP'
        WHEN 'EURO' THEN 'EUR'
        WHEN 'DOLLAR' THEN 'USD'
        WHEN 'EUR' THEN 'EUR'
        WHEN 'USD' THEN 'USD'
        WHEN 'GBP' THEN 'GBP'
        WHEN 'CHF' THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    CASE WHEN cust.kundennummer IS NOT NULL
        THEN 'A' || SUBSTRING(MD5(cust.kundennummer) FROM 1 FOR 15)
    END AS "AccountId",
    opps.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} opps
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} cust
    ON REPLACE(opps.kunden_ref, 'KD-', 'CUST-') = cust.kundennummer