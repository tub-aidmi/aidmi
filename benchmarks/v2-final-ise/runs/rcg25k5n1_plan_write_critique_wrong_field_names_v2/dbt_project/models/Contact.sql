{{ config(materialized='table') }}

SELECT
    'CON' || REGEXP_REPLACE(TRIM(a.ap_id), '^CON-?', '') AS "Id",
    INITCAP(TRIM(a.vorname)) AS "FirstName",
    CASE
        WHEN TRIM(a.nachname) = '' OR TRIM(a.nachname) IS NULL THEN 'Unknown'
        ELSE TRIM(a.nachname)
    END AS "LastName",
    LOWER(TRIM(a.email_adresse)) AS "Email",
    REGEXP_REPLACE(TRIM(a.telefonnummer), '[^0-9+]', '') AS "Phone",
    INITCAP(TRIM(a.position)) AS "Title",
    CASE UPPER(TRIM(a.funktion))
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'ENDANWENDER' THEN 'End User'
        WHEN 'TECHNISCHER ANSPRECHPARTNER' THEN 'Technical Contact'
        WHEN 'GESCHÄFTSFÜHRER/VORSTAND' THEN 'Executive Sponsor'
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(a.sprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'FRANZÖSISCH' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'SPANISCH' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        WHEN 'ITALIENISCH' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    'CUS' || REGEXP_REPLACE(TRIM(k.kunden_nr), '[^0-9]', '') AS "AccountId",
    TRIM(a.ap_id) AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP()::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP()::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(a.kunde) = TRIM(k.kunden_nr)