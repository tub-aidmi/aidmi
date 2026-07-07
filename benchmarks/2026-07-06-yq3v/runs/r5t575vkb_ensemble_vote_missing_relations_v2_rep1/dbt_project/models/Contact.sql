-- dbt model for Contact
{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    NULLIF(TRIM(SPLIT_PART(c.full_name, ' ', 1)), '') AS "FirstName",
    COALESCE(
        NULLIF(TRIM(SUBSTRING(c.full_name FROM POSITION(' ' IN c.full_name) + 1)), ''),
        NULLIF(TRIM(c.full_name), ''),
        'Unknown' -- Default for NOT NULL LastName
    ) AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    CAST(NULL AS TEXT) AS "Role__c", -- Enum type, explicitly cast NULL
    CAST(NULL AS TEXT) AS "Preferred_Language__c", -- Enum type, explicitly cast NULL
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON c.account_ref = a.id