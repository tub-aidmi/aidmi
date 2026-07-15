{{ config(materialized='table') }}

SELECT
    MD5(ap.ap_id) AS "Id",
    TRIM(vorname) AS "FirstName",
    TRIM(nachname) AS "LastName",
    TRIM(email_adresse) AS "Email",
    TRIM(telefonnummer) AS "Phone",
    TRIM(position) AS "Title",
    CASE 
        WHEN LOWER(funktion) IN ('entscheider', 'decision maker') THEN 'Decision Maker'
        WHEN LOWER(funktion) IN ('endbenutzer', 'end user') THEN 'End User'
        WHEN LOWER(funktion) IN ('technischer kontakt', 'technical contact') THEN 'Technical Contact'
        WHEN LOWER(funktion) IN ('executive sponsor', 'geschäftsführung') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN LOWER(sprache) IN ('deutsch', 'german', 'de') THEN 'DE'
        WHEN LOWER(sprache) IN ('englisch', 'english', 'en') THEN 'EN'
        WHEN LOWER(sprache) IN ('französisch', 'french', 'fr') THEN 'FR'
        WHEN LOWER(sprache) IN ('spanisch', 'spanish', 'es') THEN 'ES'
        WHEN LOWER(sprache) IN ('italienisch', 'italian', 'it') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kunde) AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
