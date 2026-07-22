{{ config(materialized='table') }}
SELECT
    LOWER(MD5(ap.ap_id)) AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    TRIM(ap.nachname) AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE 
        WHEN UPPER(TRIM(ap.funktion)) IN ('ENTSCHEIDUNGSTRÄGER', 'ENTSCHEIDUNGSTRAGER', 'DECISION MAKER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(ap.funktion)) IN ('ENDNUTZER', 'END USER') THEN 'End User'
        WHEN UPPER(TRIM(ap.funktion)) IN ('TECHNISCHER KONTAKT', 'TECHNICAL CONTACT') THEN 'Technical Contact'
        WHEN UPPER(TRIM(ap.funktion)) IN ('EXECUTIVE SPONSOR', 'GESCHÄFTSFÜHRER', 'GESCHAFTSFUHRER') THEN 'Executive Sponsor'
        ELSE NULL 
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(ap.sprache)) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(ap.sprache)) IN ('FR', 'FRANZÖSISCH', 'FRANZOSISCH') THEN 'FR'
        WHEN UPPER(TRIM(ap.sprache)) IN ('ES', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(ap.sprache)) IN ('IT', 'ITALIENISCH') THEN 'IT'
        ELSE NULL 
    END AS "Preferred_Language__c",
    LOWER(MD5(k.kunden_nr)) AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON TRIM(ap.kunde) = TRIM(k.kunden_nr)