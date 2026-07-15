{{ config(materialized='table') }}

SELECT
    TRIM(c.id) AS "Id",
    INITCAP(TRIM(SPLIT_PART(c.full_name, ' ', 1))) AS "FirstName",
    INITCAP(TRIM(CASE WHEN POSITION(' ' IN c.full_name) > 0 THEN SUBSTR(c.full_name, POSITION(' ' IN c.full_name) + 1) ELSE 'Unknown' END)) AS "LastName",
    LOWER(TRIM(COALESCE(c.email, ''))) AS "Email",
    CAST(NULL AS TEXT) AS "Phone",
    CAST(NULL AS TEXT) AS "Title",
    CAST(NULL AS TEXT) AS "Role__c",
    CAST(NULL AS TEXT) AS "Preferred_Language__c",
    TRIM(UPPER(a.id)) AS "AccountId",
    TRIM(c.id) AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON TRIM(UPPER(c.account_ref)) = TRIM(UPPER(a.id))