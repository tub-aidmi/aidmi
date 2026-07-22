{{ config(materialized='table') }}

SELECT
    MD5(chancen.chance_id) AS "Id",
    COALESCE(chancen.bezeichnung, 'Unknown Opportunity') AS "Name",
    CASE LOWER(chancen.phase)
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
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(TO_CHAR(TO_DATE(chancen.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'), TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')) AS "CloseDate",
    chancen.volumen AS "Amount",
    chancen.waehrung AS "CurrencyIsoCode",
    MD5(kunden.kunden_nr) AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON chancen.kd_nr = kunden.kunden_nr
