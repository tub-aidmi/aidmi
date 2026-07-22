{{ config(materialized='table') }}

SELECT 
    ap."ap_id" AS "Id",
    TRIM(ap."vorname") AS "FirstName",
    TRIM(ap."nachname") AS "LastName",
    TRIM(LOWER(ap."email_adresse")) AS "Email",
    TRIM(ap."telefonnummer") AS "Phone",
    TRIM(INITCAP(ap."position")) AS "Title",
    CASE 
        WHEN TRIM(LOWER(ap."funktion")) IN ('entscheider', 'entscheidungsträger') THEN 'Decision Maker'
        WHEN TRIM(LOWER(ap."funktion")) IN ('anwender', 'benutzer') THEN 'End User'
        WHEN TRIM(LOWER(ap."funktion")) IN ('technisch', 'technischer kontakt') THEN 'Technical Contact'
        WHEN TRIM(LOWER(ap."funktion")) IN ('geschäftsführung', 'executive') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN TRIM(LOWER(ap."sprache")) IN ('deutsch', 'de') THEN 'DE'
        WHEN TRIM(LOWER(ap."sprache")) IN ('englisch', 'en') THEN 'EN'
        WHEN TRIM(LOWER(ap."sprache")) IN ('französisch', 'fr') THEN 'FR'
        WHEN TRIM(LOWER(ap."sprache")) IN ('spanisch', 'es') THEN 'ES'
        WHEN TRIM(LOWER(ap."sprache")) IN ('italienisch', 'it') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    k."kunden_nr" AS "AccountId",
    ap."ap_id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(ap."kunde") = TRIM(k."kunden_nr")