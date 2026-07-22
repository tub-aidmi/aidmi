{{ config(materialized='table') }}

SELECT
    id AS "Id",
    SPLIT_PART(TRIM(full_name), ' ', 1) AS "FirstName",
    CASE
        WHEN LENGTH(TRIM(full_name)) - LENGTH(REPLACE(TRIM(full_name), ' ', '')) >= 1
            THEN TRIM(SUBSTR(TRIM(full_name), POSITION(' ' IN TRIM(full_name)) + 1))
        ELSE 'Unknown'
    END AS "LastName",
    email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    account_ref AS "AccountId", -- References Account.Id
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }}
