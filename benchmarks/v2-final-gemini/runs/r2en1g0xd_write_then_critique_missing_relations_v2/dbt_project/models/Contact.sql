-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolve

{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    CASE
        WHEN c.full_name IS NULL OR TRIM(c.full_name) = '' THEN NULL
        WHEN POSITION(' ' IN TRIM(c.full_name)) = 0 THEN TRIM(c.full_name)
        ELSE TRIM(SPLIT_PART(TRIM(c.full_name), ' ', 1))
    END AS "FirstName",
    CASE
        WHEN c.full_name IS NULL OR TRIM(c.full_name) = '' THEN 'Unknown' -- LastName is NOT NULL
        WHEN POSITION(' ' IN TRIM(c.full_name)) = 0 THEN 'Unknown' -- Only one word, default last name to 'Unknown'
        ELSE TRIM(SUBSTRING(TRIM(c.full_name) FROM POSITION(' ' IN TRIM(c.full_name)) + 1))
    END AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    c.account_ref AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c