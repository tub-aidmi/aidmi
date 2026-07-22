{{ config(materialized='table') }}

SELECT
    ap."ap_id" AS "Id",
    INITCAP(TRIM(ap."vorname")) AS "FirstName",
    INITCAP(TRIM(ap."nachname")) AS "LastName",
    TRIM(ap."email_adresse") AS "Email",
    REGEXP_REPLACE(TRIM(ap."telefonnummer"), '[^0-9+]', '', 'g') AS "Phone",
    INITCAP(TRIM(ap."position")) AS "Title",
    CASE
        WHEN UPPER(TRIM(ap."funktion")) IN ('ENTSCHEIDUNGSTRÄGER', 'ENTSCHEIDUNGSTRAGER', 'DECISION MAKER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(ap."funktion")) IN ('ENDNUTZER', 'END USER') THEN 'End User'
        WHEN UPPER(TRIM(ap."funktion")) IN ('TECHNISCHER KONTAKT', 'TECHNISCHER KONTAKT', 'TECHNICAL CONTACT') THEN 'Technical Contact'
        WHEN UPPER(TRIM(ap."funktion")) IN ('EXEKUTIVER SPONSOR', 'EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(ap."sprache")) IN ('DEUTSCH', 'DE', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(ap."sprache")) IN ('ENGLISCH', 'EN', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(ap."sprache")) IN ('FRANZÖSISCH', 'FR', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(ap."sprache")) IN ('SPANISCH', 'ES', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(ap."sprache")) IN ('ITALIENISCH', 'IT', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    k."kunden_nr" AS "AccountId",
    ap."ap_id" AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(ap."kunde") = TRIM(k."kunden_nr")