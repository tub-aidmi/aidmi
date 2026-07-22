{{ config(materialized='table') }}

WITH contacts AS (
    SELECT
        id,
        full_name,
        email,
        account_ref,
        company_name,
        INITCAP(full_name) AS "full_name_capitalized"
    FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}
),
account_lookup AS (
    SELECT id, name
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
)
SELECT
    c.id AS "Id",
    SPLIT_PART(c.full_name, ' ', 1) AS "FirstName",
    TRIM(SUBSTRING(c.full_name FROM POSITION(' ' IN c.full_name) + 1)) AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    'End User' AS "Role__c",
    NULL AS "Preferred_Language__c",
    CASE 
        WHEN c.account_ref IS NOT NULL THEN c.account_ref
        ELSE (SELECT al.id FROM account_lookup al WHERE al.name = c.company_name LIMIT 1)
    END AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contacts c
