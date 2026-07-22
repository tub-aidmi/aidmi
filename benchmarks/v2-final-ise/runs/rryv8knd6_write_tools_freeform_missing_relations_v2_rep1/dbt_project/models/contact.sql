{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    SPLIT_PART(c.full_name, ' ', 1) AS "FirstName",
    CASE
        WHEN c.full_name IS NOT NULL THEN TRIM(SUBSTRING(c.full_name FROM POSITION(' ' IN c.full_name) + 1))
        ELSE NULL
    END AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    CASE
        WHEN LOWER(c.company_name) LIKE '%&%' THEN 'Executive Sponsor'
        WHEN LOWER(c.company_name) LIKE '%gmbh%' OR LOWER(c.company_name) LIKE '%ag%' OR LOWER(c.company_name) LIKE '%kg%' THEN 'Technical Contact'
        ELSE 'End User'
    END AS "Role__c",
    CASE
        WHEN a.region IN ('DACH') THEN 'DE'
        WHEN a.region IN ('UK') THEN 'EN'
        WHEN a.region IN ('Southern Europe') THEN 'IT'
        WHEN a.region IN ('Benelux', 'Nordics') THEN 'FR'
        ELSE 'EN'
    END AS "Preferred_Language__c",
    CASE
        WHEN c.account_ref IS NOT NULL THEN c.account_ref
        ELSE NULL
    END AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a ON c.account_ref = a.id
