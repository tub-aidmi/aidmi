{{ config(materialized='table') }}

SELECT
    MD5(c.chance_id) AS "Id",
    COALESCE(TRIM(c.bezeichnung), 'Opportunity ' || c.chance_id) AS "Name",
    CASE
        WHEN TRIM(LOWER(c.phase)) = 'prospecting' THEN 'Prospecting'
        WHEN TRIM(LOWER(c.phase)) = 'qualification' THEN 'Qualification'
        WHEN TRIM(LOWER(c.phase)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN TRIM(LOWER(c.phase)) = 'value proposition' THEN 'Value Proposition'
        WHEN TRIM(LOWER(c.phase)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(c.phase)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN TRIM(LOWER(c.phase)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(c.phase)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(c.phase)) = 'closed won' THEN 'Closed Won'
        WHEN TRIM(LOWER(c.phase)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        CASE
            WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01'
    ) AS "CloseDate",
    c.volumen AS "Amount",
    TRIM(UPPER(c.waehrung)) AS "CurrencyIsoCode",
    MD5(k.kunden_nr) AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON c.kd_nr = k.kunden_nr
