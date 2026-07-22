{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    SPLIT_PART(c.full_name, ' ', 1) AS "FirstName",
    CASE WHEN POSITION(' ' IN c.full_name) > 0
         THEN TRIM(SUBSTRING(c.full_name FROM POSITION(' ' IN c.full_name) + 1))
         ELSE c.full_name END AS "LastName",
    c.email AS "Email",
    NULL::TEXT AS "Phone",
    NULL::TEXT AS "Title",
    CASE WHEN c.account_ref IS NOT NULL THEN 'End User' ELSE NULL END AS "Role__c",
    NULL::TEXT AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON c.account_ref IS NOT NULL AND a.id = c.account_ref
