{{ config(materialized='table') }}

SELECT
    ch.chance_id AS "Id",
    ch.bezeichnung AS "Name",
    COALESCE(
        CASE
            WHEN ch.phase IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost') THEN ch.phase
            ELSE 'Prospecting'
        END, 'Prospecting'
    ) AS "StageName",
    TO_CHAR(CAST(ch.abschlussdatum AS DATE), 'YYYY-MM-DD') AS "CloseDate",
    ch.volumen AS "Amount",
    ch.waehrung AS "CurrencyIsoCode",
    ch.kd_nr AS "AccountId", -- This is kunden_nr from kunden, which is the Account.Id
    ch.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS ch
