-- depends_on: {{ ref('Account') }} -- This line is commented out as per transformation rules.
{{ config(materialized='table') }}

SELECT
    kontakte.kontakt_id AS "Id",
    TRIM(kontakte.rufname) AS "FirstName",
    COALESCE(TRIM(kontakte.familienname), kontakte.kontakt_id) AS "LastName",
    LOWER(TRIM(kontakte.kontakt_email)) AS "Email",
    TRIM(kontakte.tel) AS "Phone",
    TRIM(kontakte.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(kontakte.rolle) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(kontakte.rolle) IN ('end user', 'endbenutzer') THEN 'End User'
        WHEN LOWER(kontakte.rolle) IN ('technical contact', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(kontakte.rolle) IN ('executive sponsor', 'führungskraft') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(kontakte.korrespondenzsprache)) IN ('DE', 'EN', 'FR', 'ES', 'IT')
            THEN UPPER(TRIM(kontakte.korrespondenzsprache))
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(TRIM(kontakte.kd_nummer)) AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte