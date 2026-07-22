{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    INITCAP(TRIM(contact.firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(contact.lastname)), 'Unknown') AS "LastName",
    LOWER(TRIM(contact.email)) AS "Email",
    TRIM(contact.phone) AS "Phone",
    INITCAP(TRIM(contact.title)) AS "Title",
    CASE
        WHEN TRIM(LOWER(contact.role__c)) = 'decision maker' THEN 'Decision Maker'
        WHEN TRIM(LOWER(contact.role__c)) = 'end user' THEN 'End User'
        WHEN TRIM(LOWER(contact.role__c)) = 'technical contact' THEN 'Technical Contact'
        WHEN TRIM(LOWER(contact.role__c)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(UPPER(contact.preferred_language__c)) = 'DE' THEN 'DE'
        WHEN TRIM(UPPER(contact.preferred_language__c)) = 'EN' THEN 'EN'
        WHEN TRIM(UPPER(contact.preferred_language__c)) = 'FR' THEN 'FR'
        WHEN TRIM(UPPER(contact.preferred_language__c)) = 'ES' THEN 'ES'
        WHEN TRIM(UPPER(contact.preferred_language__c)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    contact.accountid AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact
