{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}
),
account_map AS (
    SELECT id, name
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
)
SELECT
    CAST(c.id AS TEXT) AS "Id",
    TRIM(SPLIT_PART(c.full_name, ' ', 1)) AS "FirstName",
    COALESCE(
        NULLIF(REGEXP_REPLACE(c.full_name, '^(\S+)\s+', ''), ''),
        NULLIF(TRIM(SPLIT_PART(c.full_name, ' ', 2)), ''),
        TRIM(c.full_name)
     ) AS "LastName",
    COALESCE(TRIM(c.email), '') AS "Email",
    NULL::TEXT AS "Phone",
    NULL::TEXT AS "Title",
    NULL::TEXT AS "Role__c",
    NULL::TEXT AS "Preferred_Language__c",
    a.id AS "AccountId",
    CAST(c.id AS TEXT) AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM src c
LEFT JOIN account_map a ON c.account_ref = a.id