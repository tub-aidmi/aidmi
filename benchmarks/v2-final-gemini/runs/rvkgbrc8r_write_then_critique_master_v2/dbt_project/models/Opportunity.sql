-- depends_on: {{ ref('Account') }}

{{ config(materialized='table') }}

WITH opportunities_stg AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }}
)
SELECT
    opp_kennung AS "Id",
    titel AS "Name",
    CASE
        WHEN LOWER(vertriebsphase) LIKE '%prospecting%' THEN 'Prospecting'
        WHEN LOWER(vertriebsphase) LIKE '%qualification%' THEN 'Qualification'
        WHEN LOWER(vertriebsphase) LIKE '%needs analysis%' THEN 'Needs Analysis'
        WHEN LOWER(vertriebsphase) LIKE '%value proposition%' THEN 'Value Proposition'
        WHEN LOWER(vertriebsphase) LIKE '%decision makers%' THEN 'Id. Decision Makers'
        WHEN LOWER(vertriebsphase) LIKE '%perception analysis%' THEN 'Perception Analysis'
        WHEN LOWER(vertriebsphase) LIKE '%proposal%' THEN 'Proposal/Price Quote'
        WHEN LOWER(vertriebsphase) LIKE '%negotiation%' THEN 'Negotiation/Review'
        WHEN LOWER(vertriebsphase) LIKE '%closed won%' THEN 'Closed Won'
        WHEN LOWER(vertriebsphase) LIKE '%closed lost%' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    CASE
        WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(zieldatum AS DATE), 'YYYY-MM-DD') -- YYYY-MM-DD
        WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
        WHEN zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY
        ELSE TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL CloseDate
    END AS "CloseDate",
    CASE
        WHEN auftragswert IS NULL OR TRIM(auftragswert) = '' THEN NULL
        ELSE CAST(REPLACE(REPLACE(REGEXP_REPLACE(auftragswert, '[^0-9,.]', '', 'g'), '.', ''), ',', '.') AS DOUBLE PRECISION)
    END AS "Amount",
    waehrungscode AS "CurrencyIsoCode",
    kunden_ref AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    opportunities_stg