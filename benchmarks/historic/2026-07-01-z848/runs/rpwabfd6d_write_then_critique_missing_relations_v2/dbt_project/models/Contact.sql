-- {{ config(materialized='table') }}

SELECT
    id AS "Id",
    CASE
        WHEN POSITION(' ' IN TRIM(full_name)) > 0
        THEN TRIM(SUBSTRING(TRIM(full_name) FROM 1 FOR LENGTH(TRIM(full_name)) - POSITION(' ' IN REVERSE(TRIM(full_name))))) 
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN POSITION(' ' IN TRIM(full_name)) > 0
            THEN TRIM(SUBSTRING(TRIM(full_name) FROM LENGTH(TRIM(full_name)) - POSITION(' ' IN REVERSE(TRIM(full_name))) + 1))
            ELSE TRIM(full_name)
        END,
        'Unknown'
    ) AS "LastName",
    email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    account_ref AS "AccountId",
    NULL AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }}