{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    INITCAP(TRIM(lastname)) AS "LastName",
    LOWER(TRIM(email)) AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE 
        WHEN LOWER(TRIM(role__c)) IN ('decision maker', 'end user', 'technical contact', 'executive sponsor') 
            THEN INITCAP(TRIM(role__c))
        ELSE NULL 
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(preferred_language__c)) IN ('DE', 'EN', 'FR', 'ES', 'IT') 
            THEN UPPER(TRIM(preferred_language__c))
        ELSE NULL 
    END AS "Preferred_Language__c",
    TRIM(accountid) AS "AccountId",
    TRIM(id) AS "Legacy_Contact_ID__c",
    CAST(COALESCE(NULLIF('', ''::TEXT), CURRENT_TIMESTAMP::TEXT) AS TEXT) AS "CreatedDate",
    CAST(COALESCE(NULLIF('', ''::TEXT), CURRENT_TIMESTAMP::TEXT) AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
