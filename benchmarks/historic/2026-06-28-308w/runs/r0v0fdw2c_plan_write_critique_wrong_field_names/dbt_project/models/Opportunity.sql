
{{ config(materialized='table') }}

SELECT
    TRIM(c.chance_id) AS "Id",
    COALESCE(c.bezeichnung, 'Unnamed Opportunity') AS "Name",
    CASE LOWER(c.phase)
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unmatchable or NULL values
    END AS "StageName",
    COALESCE(TO_CHAR(CAST(c.abschlussdatum AS DATE), 'YYYY-MM-DD'), '1900-01-01') AS "CloseDate",
    c.volumen AS "Amount",
    UPPER(c.waehrung) AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
    TRIM(c.chance_id) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_src', 'chancen') }} AS c
LEFT JOIN
    {{ source('fixture_wrong_field_names_src', 'kunden') }} AS k
    ON c.kd_nr = k.kunden_nr
