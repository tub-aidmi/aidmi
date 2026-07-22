{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    INITCAP(LEFT(TRIM(c.firstname), 100)) AS "FirstName",
    COALESCE(INITCAP(LEFT(TRIM(c.lastname), 100)), 'Unknown') AS "LastName",
    UPPER(TRIM(c.email)) AS "Email",
    TRIM(c.phone) AS "Phone",
    INITCAP(LEFT(TRIM(c.title), 100)) AS "Title",
    CASE LOWER(TRIM(c.role__c))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    UPPER(LEFT(TRIM(c.preferred_language__c), 2)) AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c
JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a
    ON COALESCE(c.accountid, '') = COALESCE(a.erp_number__c, '')
    OR COALESCE(c.accountid, '') = COALESCE(a.legacy_customer_id__c, '')