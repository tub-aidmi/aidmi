{{ config(materialized='table') }}

WITH src AS (
    SELECT *
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)
SELECT
    -- Id: Extract numeric portion from kunden_nr, prepend CUS-
    'CUS-' || REGEXP_REPLACE(TRIM(kunden_nr), '[^0-9]', '', 'g') AS "Id",
    
    -- Name: INITCAP of firmenname, fallback to Unknown Customer
    CASE 
        WHEN TRIM(firmenname) IS NULL OR TRIM(firmenname) = '' THEN 'Unknown Customer'
        ELSE INITCAP(TRIM(firmenname))
    END AS "Name",
    
    -- ERP_Number__c: passthrough with trim
    TRIM(erp_nummer) AS "ERP_Number__c",
    
    -- Customer_Tier__c: Map German tier names to English
    CASE LOWER(TRIM(kategorie))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    
    -- Region__c: TRIM and INITCAP
    INITCAP(TRIM(gebiet)) AS "Region__c",
    
    -- Industry: TRIM and INITCAP
    INITCAP(TRIM(branche)) AS "Industry",
    
    -- Website: ensure http/https prefix
    CASE 
        WHEN webisione IS NOT NULL AND (webseite ~ '^https?://') THEN TRIM(webseite)
        WHEN webseite IS NOT NULL AND webitesse ~ '^www\.' THEN 'http://' || TRIM(webseite)
        WHEN webisse IS NOT NULL AND TRIM(webisse) != '' THEN 'https://' || TRIM(webisse)
        ELSE NULL
    END AS "Website",
    
    -- BillingCity: INITCAP and trim, null-protected
    CASE WHEN TRIM(ort) IS NOT NULL THEN INITCAP(TRIM(ort)) ELSE NULL END AS "BillingCity",
    
    -- BillingCountry: Map German country names to ISO 3166-1 alpha-2 codes
    CASE LOWER(TRIM(land))
        WHEN 'deutschland' THEN 'DE'
        when 'österreich' THEN 'AT'
        WHEN 'schweiz' THEN 'CH'
        WHEN 'vereinigte staaten' THEN 'US'
        WHEN 'kolumbien' THEN 'CO'
        WHEN 'niederlande' THEN 'NL'
        WHEN 'britisches territorium im indischen ozean' THEN 'IO'
        WHEN 'norwegen' THEN 'NO'
        WHEN 'schweden' THEN 'SE'
        WHEN 'dänemark' THEN 'DK'
        WHEN 'finland' THEN 'FI'
        WHEN 'spanien' THEN 'ES'
        WHEN 'portugal' THEN 'PT'
        WHEN 'italien' THEN 'IT'
        WHEN 'frankreich' THEN 'FR'
        WHEN 'belgien' THEN 'BE'
        WHEN 'luxemburg' THEN 'LU'
        WHEN 'irland' THEN 'IE'
        WHEN 'island' THEN 'IS'
        WHEN 'ukraine' THEN 'UA'
        WHEN 'russische föderation' THEN 'RU'
        WHEN 'türkei' THEN 'TR'
        WHEN 'polen' THEN 'PL'
        WHEN 'tschechien' THEN 'CZ'
        WHEN 'slowakei' THEN 'SK'
        WHEN 'ungarn' THEN 'HU'
        WHEN 'rumänien' THEN 'RO'
        WHEN 'bulgarien' THEN 'BG'
        WHEN 'kroatien' THEN 'HR'
        WHEN 'serbien' THEN 'RS'
        WHEN 'bosnien und herzegowina' THEN 'BA'
        WHEN 'slowenien' THEN 'SI'
        WHEN 'albanien' THEN 'AL'
        WHEN 'mazedonien' THEN 'MK'
        WHEN 'nordmazedonien' THEN 'MK'
        WHEN 'griechenland' THEN 'GR'
        WHEN 'zypern' THEN 'CY'
        WHEN 'malta' THEN 'MT'
        WHEN 'litauen' THEN 'LT'
        WHEN 'lettland' THEN 'LV'
        WHEN 'estland' THEN 'EE'
        WHEN 'ukraine' THEN 'UA'
        ELSE NULL
    END AS "BillingCountry",
    
    -- Legacy_Customer_ID__c: direct passthrough
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",
    
    -- CreatedDate/LastModifiedDate: CURRENT_TIMESTAMP as placeholder
    CURRENT_TIMESTAMP()::text AS "CreatedDate",
    CURRENT_TIMESTAMP()::text AS "LastModifiedDate",
    
    -- IsDeleted: literal 0 (no soft-delete in source)
    0 AS "IsDeleted"
FROM src
WHERE TRIM(kunden_nr) IS NOT NULL AND TRIM(kunden_nr) != ''
