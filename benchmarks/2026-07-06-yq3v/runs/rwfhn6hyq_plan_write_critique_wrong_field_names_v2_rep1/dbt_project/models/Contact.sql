{{ config(materialized='table') }}

SELECT
    TRIM(ap.ap_id) AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    COALESCE(TRIM(ap.nachname), 'Unknown Last Name') AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE UPPER(TRIM(ap.funktion))
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'ENDNUTZER' THEN 'End User'
        WHEN 'TECHNISCHER ANSPRECHPARTNER' THEN 'Technical Contact'
        WHEN 'GESCHÄFTSFÜHRER' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(ap.sprache))
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'FRANZÖSISCH' THEN 'FR'
        WHEN 'SPANISCH' THEN 'ES'
        WHEN 'ITALIENISCH' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(k.kunden_nr) AS "AccountId",
    TRIM(ap.ap_id) AS "Legacy_Contact_ID__c",
    TO_CHAR(timezone('UTC', CURRENT_TIMESTAMP), 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "CreatedDate",
    TO_CHAR(timezone('UTC', CURRENT_TIMESTAMP), 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    TRIM(ap.kunde) = TRIM(k.kunden_nr)
