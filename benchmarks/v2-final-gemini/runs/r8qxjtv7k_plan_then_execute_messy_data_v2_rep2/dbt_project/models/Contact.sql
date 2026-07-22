{{ config(materialized='table') }}

SELECT
    TRIM(contact.id) AS "Id",
    INITCAP(TRIM(contact.firstname)) AS "FirstName",
    TRIM(COALESCE(contact.lastname, 'Unknown')) AS "LastName",
    LOWER(TRIM(contact.email)) AS "Email",
    TRIM(contact.phone) AS "Phone",
    INITCAP(TRIM(contact.title)) AS "Title",
    CASE
        WHEN INITCAP(TRIM(contact.role__c)) = 'Decision Maker' THEN 'Decision Maker'
        WHEN INITCAP(TRIM(contact.role__c)) = 'End User' THEN 'End User'
        WHEN INITCAP(TRIM(contact.role__c)) = 'Technical Contact' THEN 'Technical Contact'
        WHEN INITCAP(TRIM(contact.role__c)) = 'Executive Sponsor' THEN 'Executive Sponsor'
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
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact
