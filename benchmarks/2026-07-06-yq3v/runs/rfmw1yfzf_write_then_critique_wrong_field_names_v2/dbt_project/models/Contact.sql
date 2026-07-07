{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    ap.vorname AS "FirstName",
    COALESCE(ap.nachname, 'Unknown') AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE UPPER(TRIM(ap.funktion))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'ENDBENUTZER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'TECHNISCHER KONTAKT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN 'VORSTAND' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(ap.sprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(k.kunden_nr || k.firmenname) AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    ap.kunde = k.kunden_nr
