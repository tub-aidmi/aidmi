{{ config(materialized='table') }}

WITH opportunity_base AS (
    SELECT
        c.chance_id,
        c.bezeichnung,
        c.phase,
        c.abschlussdatum,
        c.volumen,
        c.waehrung,
        c.kd_nr,
        k.kunden_nr AS account_kunden_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
        ON c.kd_nr = k.kunden_nr
)

SELECT
    MD5(o.chance_id) AS "Id",
    COALESCE(TRIM(o.bezeichnung), 'Unnamed Opportunity') AS "Name",
    o.phase AS "StageName",
    o.abschlussdatum AS "CloseDate",
    o.volumen AS "Amount",
    UPPER(TRIM(o.waehrung)) AS "CurrencyIsoCode",
    o.account_kunden_nr AS "AccountId",
    o.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_base o