{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    COALESCE(c.bezeichnung, 'Unknown') AS "Name",
    CASE
        WHEN INITCAP(TRIM(c.phase)) IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost')
            THEN INITCAP(TRIM(c.phase))
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN c.abschlussdatum IS NOT NULL AND c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$'
            THEN c.abschlussdatum
        WHEN c.abschlussdatum IS NOT NULL AND c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    END AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    '001' || SUBSTRING(MD5(c.kd_nr), 1, 15) AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c