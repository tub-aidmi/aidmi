-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}

{{ config(materialized='table') }}

SELECT
    ENCODE(SHA256(proj.proj_id::BYTEA), 'hex') AS "Id",
    TRIM(proj.name) AS "Name",
    CASE
        WHEN UPPER(TRIM(proj.status)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(proj.status)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(proj.status)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(proj.status)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(proj.status)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    COALESCE(
        TO_CHAR(TO_DATE(proj.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(proj.go_live, 'YYYYMMDD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(proj.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        NULL
    ) AS "Go_Live_Date__c",
    ENCODE(SHA256(kunden.kunden_nr::BYTEA), 'hex') AS "Account__c",
    ENCODE(SHA256(chancen.chance_id::BYTEA), 'hex') AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON proj.kd = kunden.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
    ON proj.opp = chancen.chance_id