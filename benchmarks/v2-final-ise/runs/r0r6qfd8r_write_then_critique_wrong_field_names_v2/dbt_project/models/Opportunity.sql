{{ config(materialized='table') }}
SELECT
    c."chance_id" AS "Id",
    c."bezeichnung" AS "Name",
    CASE
        WHEN UPPER(TRIM(c."phase")) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(c."phase")) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(c."phase")) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(c."phase")) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(c."phase")) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(c."phase")) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(c."phase")) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(c."phase")) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(c."phase")) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(c."phase")) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN c."abschlussdatum" ~ '^\d{4}-\d{2}-\d{2}$' THEN c."abschlussdatum"
        ELSE NULL
    END AS "CloseDate",
    c."volumen" AS "Amount",
    c."waehrung" AS "CurrencyIsoCode",
    SUBSTRING(MD5(k."kunden_nr") FROM 1 FOR 18) AS "AccountId",
    c."chance_id" AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON c."kd_nr" = k."kunden_nr"