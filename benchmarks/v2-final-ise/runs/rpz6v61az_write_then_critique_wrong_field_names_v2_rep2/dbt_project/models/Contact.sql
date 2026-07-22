{{ config(materialized='table') }}
SELECT
    ap."ap_id" AS "Id",
    TRIM(INITCAP(ap."vorname")) AS "FirstName",
    TRIM(INITCAP(ap."nachname")) AS "LastName",
    TRIM(LOWER(ap."email_adresse")) AS "Email",
    TRIM(ap."telefonnummer") AS "Phone",
    TRIM(INITCAP(ap."position")) AS "Title",
    CASE
        WHEN TRIM(LOWER(ap."funktion")) IN ('decision maker', 'entscheidungsträger') THEN 'Decision Maker'
        WHEN TRIM(LOWER(ap."funktion")) IN ('end user', 'endbenutzer') THEN 'End User'
        WHEN TRIM(LOWER(ap."funktion")) IN ('technical contact', 'technischer kontakt') THEN 'Technical Contact'
        WHEN TRIM(LOWER(ap."funktion")) IN ('executive sponsor', 'exekutiver sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(UPPER(ap."sprache")) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN TRIM(UPPER(ap."sprache")) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN TRIM(UPPER(ap."sprache")) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN TRIM(UPPER(ap."sprache")) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
        WHEN TRIM(UPPER(ap."sprache")) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CASE
        WHEN k."kunden_nr" IS NOT NULL THEN md5(k."kunden_nr" || 'salt')::uuid::text
        ELSE NULL
    END AS "AccountId",
    ap."ap_id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(ap."kunde") = TRIM(k."kunden_nr")