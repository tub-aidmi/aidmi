{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    COALESCE(c.bezeichnung, c.chance_id) AS "Name",
    CASE
        WHEN c.phase IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost') THEN c.phase
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        CASE
            WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(c.abschlussdatum AS DATE), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON c.kd_nr = k.kunden_nr
