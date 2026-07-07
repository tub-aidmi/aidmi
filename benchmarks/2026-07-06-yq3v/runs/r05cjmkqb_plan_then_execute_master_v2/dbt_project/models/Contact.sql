-- depends_on: {{ ref('Account') }}
{{ config(materialized='table') }}

SELECT
    MD5(TRIM(mk.kontakt_id)) AS "Id",
    TRIM(INITCAP(mk.rufname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(mk.familienname)), 'Unknown') AS "LastName",
    TRIM(LOWER(mk.kontakt_email)) AS "Email",
    TRIM(mk.tel) AS "Phone",
    TRIM(mk.berufsbezeichnung) AS "Title",
    CASE TRIM(UPPER(mk.rolle))
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'ENDNUTZER' THEN 'End User'
        WHEN 'TECHNISCHER ANSPRECHPARTNER' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE TRIM(UPPER(mk.korrespondenzsprache))
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'FRANZÖSISCH' THEN 'FR'
        WHEN 'SPANISCH' THEN 'ES'
        WHEN 'ITALIENISCH' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(TRIM(mk.kd_nummer)) AS "AccountId",
    TRIM(mk.kontakt_id) AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk