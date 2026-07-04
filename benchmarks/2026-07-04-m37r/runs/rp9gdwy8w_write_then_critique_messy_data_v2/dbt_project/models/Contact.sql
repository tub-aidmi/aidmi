-- {{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    contact.firstname AS "FirstName",
    COALESCE(contact.lastname, 'Unknown') AS "LastName",
    contact.email AS "Email",
    contact.phone AS "Phone",
    contact.title AS "Title",
    CASE
        WHEN LOWER(contact.role__c) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(contact.role__c) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(contact.role__c) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(contact.role__c) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(contact.preferred_language__c) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(contact.preferred_language__c) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(contact.preferred_language__c) IN ('fr', 'french', 'französisch') THEN 'FR'
        WHEN LOWER(contact.preferred_language__c) IN ('es', 'spanish', 'spanisch') THEN 'ES'
        WHEN LOWER(contact.preferred_language__c) IN ('it', 'italian', 'italienisch') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    contact.accountid AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact