{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    SPLIT_PART(c.full_name, ' ', 1) AS "FirstName",
    COALESCE(NULLIF(SPLIT_PART(c.full_name, ' ', 2), ''), INITCAP(c.full_name)) AS "LastName",
    c.email AS "Email",
    NULL::text AS "Phone",
    NULL::text AS "Title",
    CASE
        WHEN c.account_ref IS NOT NULL THEN 'End User'
        ELSE NULL
    END AS "Role__c",
    NULL::text AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON c.account_ref = a.id