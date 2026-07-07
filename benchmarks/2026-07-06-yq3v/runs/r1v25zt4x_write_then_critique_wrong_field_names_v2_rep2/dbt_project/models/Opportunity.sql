-- depends_on: {{ ref('Account') }}
{{ config(materialized='table') }}

WITH opportunities AS (
    SELECT
        chance_id,
        bezeichnung,
        phase,
        abschlussdatum,
        volumen,
        waehrung,
        kd_nr
    FROM
        {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
)
SELECT
    o.chance_id AS "Id",
    COALESCE(o.bezeichnung, 'Unknown Opportunity Name') AS "Name",
    CASE
        WHEN LOWER(TRIM(o.phase)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(o.phase)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(o.phase)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.phase)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.phase)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.phase)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.phase)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.phase)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.phase)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(o.phase)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL column
    END AS "StageName",
    COALESCE(
        CASE WHEN o.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(o.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD') ELSE NULL END,
        CASE WHEN o.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD') ELSE NULL END,
        CASE WHEN o.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD') ELSE NULL END,
        '1970-01-01' -- Explicit default for NOT NULL date if unparseable, avoids sentinel current date
    ) AS "CloseDate",
    o.volumen AS "Amount",
    o.waehrung AS "CurrencyIsoCode",
    MD5(o.kd_nr) AS "AccountId",
    o.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    opportunities AS o