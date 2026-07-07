-- models/Opportunity.sql

{{ config(materialized='table') }}

WITH account_ids AS (
    SELECT
        kundennummer AS "Id",
        kundennummer AS "Legacy_Customer_ID__c"
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
    opp.opp_kennung AS "Id",
    COALESCE(TRIM(opp.titel), 'N/A') AS "Name",
    CASE
        WHEN opp.vertriebsphase = 'Prospecting' THEN 'Prospecting'
        WHEN opp.vertriebsphase = 'Qualification' THEN 'Qualification'
        WHEN opp.vertriebsphase = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN opp.vertriebsphase = 'Value Proposition' THEN 'Value Proposition'
        WHEN opp.vertriebsphase = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN opp.vertriebsphase = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN opp.vertriebsphase = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN opp.vertriebsphase = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN opp.vertriebsphase = 'Closed Won' THEN 'Closed Won'
        WHEN opp.vertriebsphase = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for StageName as it is NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN opp.zieldatum
            WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN opp.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE '1900-01-01' -- Default for unparseable dates as target is NOT NULL
        END,
        '1900-01-01' -- Fallback if zieldatum is NULL, as target is NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN opp.auftragswert IS NULL OR opp.auftragswert = '' THEN NULL
        ELSE CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(opp.auftragswert), '[^0-9,.]', '', 'g'), ',', '.') AS DOUBLE PRECISION)
    END AS "Amount",
    opp.waehrungscode AS "CurrencyIsoCode",
    acc."Id" AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp
LEFT JOIN account_ids AS acc
    ON opp.kunden_ref = acc."Legacy_Customer_ID__c"