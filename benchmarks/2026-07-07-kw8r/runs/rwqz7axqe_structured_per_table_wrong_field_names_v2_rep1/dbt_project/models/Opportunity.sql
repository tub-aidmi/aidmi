{{ config(materialized='table') }}

SELECT
    CAST(c.chance_id AS TEXT) AS "Id",
    INITCAP(c.bezeichnung) AS "Name",
    CASE c.phase
        WHEN 'Prospecting' THEN 'Prospecting'
        WHEN 'Qualification' THEN 'Qualification'
        WHEN 'Needs Analysis' THEN 'Needs Analysis'
        WHEN 'Value Proposition' THEN 'Value Proposition'
        WHEN 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN 'Perception Analysis' THEN 'Perception Analysis'
        WHEN 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN 'Closed Won' THEN 'Closed Won'
        WHEN 'Closed Lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN c.abschlussdatum IS NOT NULL AND c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TO_DATE(c.abschlussdatum, 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CAST(c.volumen AS DOUBLE PRECISION) AS "Amount",
    UPPER(c.waehrung) AS "CurrencyIsoCode",
    -- AccountId: transform CUST-XXXX to the numeric key used by Account.Id
    REGEXP_REPLACE(k.kunden_nr, '^CUST-', '') AS "AccountId",
    CAST(c.chance_id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON c.kd_nr = k.kunden_nr