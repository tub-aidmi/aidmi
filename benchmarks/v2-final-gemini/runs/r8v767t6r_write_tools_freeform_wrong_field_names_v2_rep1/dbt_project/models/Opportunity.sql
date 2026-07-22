{{ config(materialized='table') }}

SELECT
    MD5(c.chance_id) AS "Id",
    c.bezeichnung AS "Name",
    CASE
        WHEN TRIM(c.phase) IN (
            'Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition',
            'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote',
            'Negotiation/Review', 'Closed Won', 'Closed Lost'
        )
        THEN TRIM(c.phase)
        ELSE NULL -- Or a sensible default if target requires NOT NULL
    END AS "StageName",
    COALESCE(c.abschlussdatum, '1900-01-01') AS "CloseDate", -- Target is NOT NULL, so provide a default
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    MD5(k.kunden_nr) AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    c.kd_nr = k.kunden_nr
