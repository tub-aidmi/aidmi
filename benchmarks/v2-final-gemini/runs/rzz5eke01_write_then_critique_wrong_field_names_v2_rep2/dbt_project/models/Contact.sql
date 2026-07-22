-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    TRIM(ap.ap_id) AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    COALESCE(TRIM(ap.nachname), 'Unknown') AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE UPPER(TRIM(ap.funktion))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'ENDBENUTZER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'TECHNISCHER KONTAKT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN 'FÜHRUNGSKRAFT' THEN 'Executive Sponsor'
        WHEN 'GESCHÄFTSFÜHRER' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(ap.sprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'ENGLISH' THEN 'EN'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'FRENCH' THEN 'FR'
        WHEN 'FRANZÖSISCH' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'SPANISH' THEN 'ES'
        WHEN 'SPANISCH' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        WHEN 'ITALIAN' THEN 'IT'
        WHEN 'ITALIENISCH' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(k.kunden_nr) AS "AccountId",
    TRIM(ap.ap_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON TRIM(ap.kunde) = TRIM(k.kunden_nr)
