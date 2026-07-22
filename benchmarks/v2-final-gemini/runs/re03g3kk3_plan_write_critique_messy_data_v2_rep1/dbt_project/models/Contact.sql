{{ config(materialized='table') }}

SELECT
    TRIM(contact.id) AS "Id",
    TRIM(contact.firstname) AS "FirstName",
    COALESCE(TRIM(contact.lastname), 'N/A') AS "LastName",
    LOWER(TRIM(contact.email)) AS "Email",
    TRIM(contact.phone) AS "Phone",
    TRIM(contact.title) AS "Title",
    CASE
        WHEN LOWER(TRIM(contact.role__c)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(contact.role__c)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(contact.role__c)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(contact.role__c)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(contact.preferred_language__c)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(contact.preferred_language__c)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(contact.preferred_language__c)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(contact.preferred_language__c)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(contact.preferred_language__c)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(contact.accountid) AS "AccountId",
    TRIM(contact.id) AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact
