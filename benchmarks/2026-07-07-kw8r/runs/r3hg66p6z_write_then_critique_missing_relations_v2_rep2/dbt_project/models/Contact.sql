{{ config(materialized='table') }}
SELECT
    c.id AS "Id",
    CASE
        WHEN c.full_name IS NULL THEN NULL
        WHEN c.full_name ~ '^\S+\s' THEN
            REGEXP_REPLACE(
                c.full_name,
                '^(\S+)(?:\s|-)(.*)$',
                '\1'
            )
        ELSE c.full_name
    END AS "FirstName",
    CASE
        WHEN c.full_name IS NULL THEN 'Unknown'
        WHEN c.full_name ~ '^\S+\s' THEN
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    c.full_name,
                    '^\S+(?:\s|-)(.*)$',
                    '\1'
                ),
                '^\s*(.*)$',
                '\1'
            )
        ELSE NULL
    END AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    'End User' AS "Role__c",
    'EN' AS "Preferred_Language__c",
    COALESCE(a_account_ref.id, a_company_name.id) AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a_account_ref
    ON c.account_ref = a_account_ref.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a_company_name
    ON TRIM(c.company_name) = TRIM(a_company_name.name)