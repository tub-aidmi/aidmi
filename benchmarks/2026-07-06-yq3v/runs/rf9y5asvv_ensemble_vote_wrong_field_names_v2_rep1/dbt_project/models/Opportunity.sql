-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}

SELECT
    chance_id AS "Id",
    COALESCE(bezeichnung, 'Unnamed Opportunity') AS "Name",
    COALESCE(phase, 'Prospecting') AS "StageName",
    COALESCE(abschlussdatum, '1900-01-01') AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    kd_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}