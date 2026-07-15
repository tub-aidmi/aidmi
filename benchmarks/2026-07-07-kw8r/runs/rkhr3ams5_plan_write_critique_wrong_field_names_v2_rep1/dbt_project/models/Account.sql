{{ config(materialized='table') }}

SELECT
    '001' || LPAD(REGEXP_REPLACE(TRIM(kunden_nr), '[^0-9]', '', 'g'), 15, '0') AS "Id",
    COALESCE(NULLIF(INITCAP(TRIM(firmenname)), ''), 'Unknown Customer') AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE UPPER(TRIM(kategorie))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(gebiet) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    CASE
        WHEN webseite IS NULL OR TRIM(webseite) = '' THEN NULL
        WHEN LOWER(TRIM(webseite)) LIKE 'http%' THEN
            'https://' || SUBSTR(TRIM(webseite), POSITION('//' IN TRIM(webseite)) + 2)
        ELSE 'https://' || TRIM(webseite)
    END AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    CASE UPPER(TRIM(land))
        WHEN 'DEUTSCHLAND' THEN 'DE'
        WHEN 'ÖSTERREICH' THEN 'AT'
        WHEN 'SCHWEIZ' THEN 'CH'
        WHEN 'ITALIEN' THEN 'IT'
        WHEN 'FRANKREICH' THEN 'FR'
        WHEN 'SPANIEN' THEN 'ES'
        WHEN 'PORTUGAL' THEN 'PT'
        WHEN 'NIEDERLANDE' THEN 'NL'
        WHEN 'BELGIEN' THEN 'BE'
        WHEN 'LUXEMBURG' THEN 'LU'
        WHEN 'DÄNEMARK' THEN 'DK'
        WHEN 'SCHWEDEN' THEN 'SE'
        WHEN 'NORWEGEN' THEN 'NO'
        WHEN 'FINNLAND' THEN 'FI'
        WHEN 'POLSEN' THEN 'PL'
        WHEN 'POLEN' THEN 'PL'
        WHEN 'TSCHECHIEN' THEN 'CZ'
        WHEN 'UNGARN' THEN 'HU'
        WHEN 'RUMÄNIEN' THEN 'RO'
        WHEN 'BULGARIEN' THEN 'BG'
        WHEN 'GREICHENLAND' THEN 'GR'
        WHEN 'GREECE' THEN 'GR'
        WHEN 'VEREINIGTES KÖNIGREICH' THEN 'GB'
        WHEN 'IRLAND' THEN 'IE'
        WHEN 'ISLAND' THEN 'IS'
        WHEN 'LITAUEN' THEN 'LT'
        WHEN 'LETTLAND' THEN 'LV'
        WHEN 'ESTLAND' THEN 'EE'
        WHEN 'KROATIEN' THEN 'HR'
        WHEN 'SERBIEN' THEN 'RS'
        WHEN 'BOSNIEN UND HERZEGOWINA' THEN 'BA'
        ELSE NULL
    END AS "BillingCountry",
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "CreatedDate",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}