{{ config(materialized='table') }}

SELECT
    ap_id AS "Id",
    vorname AS "FirstName",
    nachname AS "LastName",
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    CASE
        WHEN UPPER(funktion) IN ('DECISION MAKER', 'END USER', 'TECHNICAL CONTACT', 'EXECUTIVE SPONSOR') THEN INITCAP(LOWER(funktion))
        WHEN UPPER(funktion) IN ('ENTSCHEIDUNGSTRÄGER', 'ENTSCHEIDUNGSTRAGER') THEN 'Decision Maker'
        WHEN UPPER(funktion) IN ('ENDNUTZER') THEN 'End User'
        WHEN UPPER(funktion) IN ('TECHNISCHER KONTAKT', 'TECHNISCH') THEN 'Technical Contact'
        WHEN UPPER(funktion) IN ('GESCHÄFTSFÜHRUNG', 'GESCHAFTSFUHRUNG') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(sprache) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(sprache)
        WHEN UPPER(sprache) IN ('DEUTSCH') THEN 'DE'
        WHEN UPPER(sprache) IN ('ENGLISCH') THEN 'EN'
        WHEN UPPER(sprache) IN ('FRANZÖSISCH', 'FRANZOSISCH') THEN 'FR'
        WHEN UPPER(sprache) IN ('SPANISCH') THEN 'ES'
        WHEN UPPER(sprache) IN ('ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    k.kunden_nr AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON a.kunde = k.kunden_nr
