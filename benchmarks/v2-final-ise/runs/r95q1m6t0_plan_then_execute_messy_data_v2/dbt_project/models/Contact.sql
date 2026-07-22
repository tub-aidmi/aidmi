{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    INITCAP(TRIM(c.firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(c.lastname)), 'Unknown') AS "LastName",
    TRIM(c.email) AS "Email",
    TRIM(c.phone) AS "Phone",
    TRIM(c.title) AS "Title",
    CASE
        WHEN INITCAP(TRIM(c.role__c)) IN ('Decision Maker', 'End User', 'Technical Contact', 'Executive Sponsor')
            THEN INITCAP(TRIM(c.role__c))
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(c.preferred_language__c)) IN ('DE', 'EN', 'FR', 'ES', 'IT')
            THEN UPPER(TRIM(c.preferred_language__c))
        ELSE NULL
    END AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON c.accountid = a.id