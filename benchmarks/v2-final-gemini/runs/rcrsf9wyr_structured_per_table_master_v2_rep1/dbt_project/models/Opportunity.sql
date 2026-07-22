-- depends_on: {{ ref('Account') }}

{{ config(materialized='table') }}

WITH opportunities_stg AS (
    SELECT
        opp.opp_kennung,
        opp.titel,
        opp.vertriebsphase,
        opp.zieldatum,
        opp.auftragswert,
        opp.waehrungscode,
        opp.kunden_ref
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp
)
SELECT
    opp_stg.opp_kennung AS "Id",
    COALESCE(TRIM(opp_stg.titel), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN opp_stg.vertriebsphase ILIKE '%prospecting%' THEN 'Prospecting'
        WHEN opp_stg.vertriebsphase ILIKE '%qualification%' THEN 'Qualification'
        WHEN opp_stg.vertriebsphase ILIKE '%needs analysis%' THEN 'Needs Analysis'
        WHEN opp_stg.vertriebsphase ILIKE '%value proposition%' THEN 'Value Proposition'
        WHEN opp_stg.vertriebsphase ILIKE '%decision makers%' THEN 'Id. Decision Makers'
        WHEN opp_stg.vertriebsphase ILIKE '%perception analysis%' THEN 'Perception Analysis'
        WHEN opp_stg.vertriebsphase ILIKE '%proposal/price quote%' THEN 'Proposal/Price Quote'
        WHEN opp_stg.vertriebsphase ILIKE '%negotiation/review%' THEN 'Negotiation/Review'
        WHEN opp_stg.vertriebsphase ILIKE '%closed won%' THEN 'Closed Won'
        WHEN opp_stg.vertriebsphase ILIKE '%closed lost%' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to 'Prospecting' if not mapped, as StageName is NOT NULL
    END AS "StageName",
    COALESCE(TO_CHAR(TO_DATE(opp_stg.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'), '1900-01-01') AS "CloseDate", -- Target NOT NULL, use fallback for unparseable dates
    CASE
        WHEN opp_stg.auftragswert ~ '^[0-9]+([\.][0-9]+)?$' THEN CAST(opp_stg.auftragswert AS DOUBLE PRECISION)
        WHEN opp_stg.auftragswert ~ '^[0-9]+\,[0-9]+$' THEN CAST(REPLACE(opp_stg.auftragswert, ',', '.') AS DOUBLE PRECISION)
        WHEN opp_stg.auftragswert ~ '^[0-9]{1,3}(\.[0-9]{3})*\,[0-9]+$' THEN CAST(REPLACE(REPLACE(opp_stg.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    opp_stg.waehrungscode AS "CurrencyIsoCode",
    opp_stg.kunden_ref AS "AccountId", -- Maps directly to Account.Id which is Legacy_Customer_ID__c
    opp_stg.opp_kennung AS "Legacy_Opportunity_ID__c",
    '1900-01-01 00:00:00' AS "CreatedDate", -- Placeholder, no source for created date
    '1900-01-01 00:00:00' AS "LastModifiedDate", -- Placeholder, no source for last modified date
    0 AS "IsDeleted"
FROM
    opportunities_stg AS opp_stg
