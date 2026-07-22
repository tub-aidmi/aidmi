{{ config(materialized='table') }}

WITH contact AS (
  SELECT * FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
),
account AS (
  SELECT * FROM {{ source('fixture_messy_data_v2_src', 'account') }}
)

SELECT
    TRIM(UPPER(c.id)) AS "Id",
    INITCAP(TRIM(c.firstname)) AS "FirstName",
    CASE 
        WHEN TRIM(c.lastname) IS NULL OR TRIM(c.lastname) = '' THEN 'Unknown' 
        ELSE INITCAP(TRIM(c.lastname)) 
    END AS "LastName",
    LOWER(TRIM(c.email)) AS "Email",
    TRIM(c.phone) AS "Phone",
    INITCAP(TRIM(c.title)) AS "Title",
    CASE UPPER(TRIM(c.role__c))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(c.preferred_language__c))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(UPPER(ac.id)) AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact c
LEFT JOIN account ac 
    ON TRIM(UPPER(c.accountid)) = TRIM(UPPER(ac.id))