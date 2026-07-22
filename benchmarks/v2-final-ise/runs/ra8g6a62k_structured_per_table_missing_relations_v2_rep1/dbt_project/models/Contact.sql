{{ config(materialized='table') }}

WITH contact_account_mapping AS (
    SELECT
        c.id AS contact_id,
        c.full_name,
        c.email,
        c.account_ref,
        c.company_name,
        COALESCE(
            a.id,
            a2.id
        ) AS account_id
    FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
    LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
        ON c.account_ref = a.id
    LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a2
        ON c.account_ref IS NULL AND c.company_name = a2.name
)

SELECT
    cam.contact_id AS "Id",
    CASE
        WHEN cam.full_name ~ ' ' THEN
            SUBSTRING(cam.full_name FROM 1 FOR POSITION(' ' IN cam.full_name) - 1)
        ELSE NULL
    END AS "FirstName",
    CASE
        WHEN cam.full_name ~ ' ' THEN
            SUBSTRING(cam.full_name FROM POSITION(' ' IN cam.full_name) + 1)
        ELSE cam.full_name
    END AS "LastName",
    cam.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    cam.account_id AS "AccountId",
    cam.contact_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact_account_mapping cam