{{ config(materialized='table') }}

SELECT
    TRIM(ap.ap_id) AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    COALESCE(TRIM(ap.nachname), 'Unknown') AS "LastName",
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE TRIM(UPPER(ap.funktion))
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'ENDNUTZER' THEN 'End User'
        WHEN 'TECHNISCHER_ANSPRECHPARTNER' THEN 'Technical Contact'
        WHEN 'GESCHAEFTSFUEHRER' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE TRIM(UPPER(ap.sprache))
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'FRANZOESISCH' THEN 'FR'
        WHEN 'SPANISCH' THEN 'ES'
        WHEN 'ITALIENISCH' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(ap.kunde) AS "AccountId",
    TRIM(ap.ap_id) AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
