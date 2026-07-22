{{ config(materialized='table') }}

SELECT 
    CAST(c.id AS TEXT) AS "Id",
    CASE 
        WHEN c.full_name IS NOT NULL AND POSITION(' ' IN c.full_name) > 0 
        THEN TRIM(LEFT(c.full_name, POSITION(' ' IN c.full_name) - 1))
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        NULLIF(
            CASE 
                WHEN c.full_name IS NOT NULL AND POSITION(' ' IN c.full_name) > 0 
                THEN TRIM(SUBSTRING(c.full_name FROM POSITION(' ' IN c.full_name) + 1))
                ELSE c.full_name
            END,
            ''
        ),
        'Unknown'
    ) AS "LastName",
    LOWER(CAST(NULLIF(c.email, '') AS TEXT)) AS "Email",
    CAST(NULL AS TEXT) AS "Phone",
    CAST(NULL AS TEXT) AS "Title",
    CAST(NULL AS TEXT) AS "Role__c",
    CAST(NULL AS TEXT) AS "Preferred_Language__c",
    a.id AS "AccountId",
    CAST(c.id AS TEXT) AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON c.account_ref = a.id