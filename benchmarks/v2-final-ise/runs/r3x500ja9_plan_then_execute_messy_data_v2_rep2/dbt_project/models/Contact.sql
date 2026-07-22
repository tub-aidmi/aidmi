{{ config(materialized='table') }}

SELECT 
    INITCAP(TRIM(id)) AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    INITCAP(TRIM(lastname)) AS "LastName",
    CASE WHEN LOWER(TRIM(email)) IN ('n/a', '') THEN NULL ELSE LOWER(TRIM(email)) END AS "Email",
    CASE 
        WHEN phone IS NOT NULL AND UPPER(TRIM(phone)) != 'N/A' 
        THEN REGEXP_REPLACE(TRIM(phone), '[^\d\+]', '', 'g') 
        ELSE NULL 
    END AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE LOWER(TRIM(role__c))
        WHEN 'decision maker'     THEN 'Decision Maker'
        WHEN 'end user'           THEN 'End User'
        WHEN 'endanwender'        THEN 'End User'
        WHEN 'entscheider'        THEN 'Decision Maker'
        WHEN 'executive sponsor'  THEN 'Executive Sponsor'
        WHEN 'sponsor'            THEN 'Executive Sponsor'
        WHEN 'technical contact'  THEN 'Technical Contact'
        WHEN 'technischer ansprechpartner' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE LOWER(TRIM(preferred_language__c))
        WHEN 'de'          THEN 'DE'
        WHEN 'german'      THEN 'DE'
        WHEN 'deutsch'     THEN 'DE'
        WHEN 'en'          THEN 'EN'
        WHEN 'english'     THEN 'EN'
        WHEN 'englisch'    THEN 'EN'
        WHEN 'fr'          THEN 'FR'
        WHEN 'french'      THEN 'FR'
        WHEN 'französisch' THEN 'FR'
        WHEN 'es'          THEN 'ES'
        WHEN 'spanish'     THEN 'ES'
        WHEN 'spanisch'    THEN 'ES'
        WHEN 'it'          THEN 'IT'
        WHEN 'italian'     THEN 'IT'
        WHEN 'italienisch' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(UPPER(REGEXP_REPLACE(TRIM(accountid), '\D', '', 'g'))) AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}