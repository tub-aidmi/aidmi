{{
    config(materialized='table')
}}

WITH renovating_contacts AS (
    SELECT
        c.id,
        TRIM(c.full_name) AS full_name_cleaned,
        c.email,
        c.account_ref,
        c.company_name,
        CASE
            WHEN TRIM(c.full_name) IS NULL OR TRIM(c.full_name) = '' THEN ARRAY[]::text[]
            ELSE REGEXP_SPLIT_TO_ARRAY(TRIM(c.full_name), E'\\s+')
        END AS name_parts
    FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c
)
SELECT
    rc.id AS "Id",
    CASE
        WHEN rc.full_name_cleaned IS NULL OR array_length(rc.name_parts, 1) = 0 THEN NULL
        ELSE rc.name_parts[1]
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN rc.full_name_cleaned IS NULL OR array_length(rc.name_parts, 1) = 0 THEN NULL
            ELSE rc.name_parts[array_upper(rc.name_parts, 1)]
        END,
        rc.company_name,
        'Unknown'
    ) AS "LastName",
    rc.email AS "Email",
    NULL::text AS "Phone",
    NULL::text AS "Title",
    NULL::text AS "Role__c",
    NULL::text AS "Preferred_Language__c",
    a.id AS "AccountId",
    rc.id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM renovating_contacts AS rc
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON rc.account_ref = a.id