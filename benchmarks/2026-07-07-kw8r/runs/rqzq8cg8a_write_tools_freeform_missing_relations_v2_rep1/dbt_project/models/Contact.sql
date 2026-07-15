{{ config(materialized='table') }}

SELECT
    id AS "Id",
    SPLIT_PART(full_name, ' ', 1) AS "FirstName",
    CASE 
        WHEN POSITION(' ' IN full_name) > 0 THEN TRIM(SUBSTRING(full_name FROM POSITION(' ' IN full_name) + 1))
        ELSE ''
    END AS "LastName",
    email AS "Email",
    NULL::TEXT AS "Phone",
    company_name AS "Title",
    CASE LOWER(TRIM(company_name))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN LOWER(email) LIKE '%@de%' OR full_name ~ '[äöüÄÖÜß]' THEN 'DE'
        WHEN LOWER(email) LIKE '%@fr%' OR full_name ~ '[éèêëàâæœùûüôîïçÀ-Ù]' THEN 'FR'
        WHEN LOWER(email) LIKE '%@es%' OR full_name ~ '[áéíóúñ¿¡ÁÉÍÓÚÑ]' THEN 'ES'
        WHEN LOWER(email) LIKE '%@it%' OR full_name ~ '[àèéìòùÀÈÉÌÒÙ]' THEN 'IT'
        ELSE 'EN'
    END AS "Preferred_Language__c",
    NULL::TEXT AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}
