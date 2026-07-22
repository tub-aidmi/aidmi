{{ config(materialized='table') }}

SELECT
    MD5(TRIM(opp_kennung)) AS "Id",
    COALESCE(TRIM(titel), 'Unknown Opportunity') AS "Name",
    CASE LOWER(TRIM(vertriebsphase))
        WHEN 'in kontakt' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'quali' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'
        WHEN 'lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unknown stages, as StageName is NOT NULL
    END AS "StageName",
    COALESCE(
        CAST(CASE
            WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum -- YYYY-MM-DD
            WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END AS text),
        CURRENT_DATE::text
    ) AS "CloseDate",
    CASE
        WHEN TRIM(auftragswert) = 'None' OR auftragswert IS NULL THEN NULL
        ELSE
            CAST(REGEXP_REPLACE(TRIM(auftragswert), '[^0-9.-]+', '', 'g') AS DOUBLE PRECISION)
    END AS "Amount",
    CASE LOWER(TRIM(waehrungscode))
        WHEN 'eur' THEN 'EUR'
        WHEN 'euro' THEN 'EUR'
        WHEN '€' THEN 'EUR'
        WHEN 'usd' THEN 'USD'
        WHEN '$' THEN 'USD'
        WHEN 'dollar' THEN 'USD'
        WHEN 'chf' THEN 'CHF'
        WHEN 'gbp' THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    MD5(TRIM(kunden_ref)) AS "AccountId",
    TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}