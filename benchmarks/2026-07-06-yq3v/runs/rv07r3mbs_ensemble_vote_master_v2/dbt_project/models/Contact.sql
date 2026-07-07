{{ config(materialized='table') }}

SELECT
    MD5(kontakte.kontakt_id) AS "Id",
    TRIM(kontakte.rufname) AS "FirstName",
    COALESCE(TRIM(kontakte.familienname), '') AS "LastName",
    LOWER(TRIM(kontakte.kontakt_email)) AS "Email",
    TRIM(kontakte.tel) AS "Phone",
    TRIM(kontakte.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(TRIM(kontakte.rolle)) = 'entscheidungsträger' THEN 'Decision Maker'
        WHEN LOWER(TRIM(kontakte.rolle)) = 'endbenutzer' THEN 'End User'
        WHEN LOWER(TRIM(kontakte.rolle)) = 'technischer ansprechpartner' THEN 'Technical Contact'
        WHEN LOWER(TRIM(kontakte.rolle)) = 'führungskraft' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(kontakte.korrespondenzsprache)) = 'deutsch' THEN 'DE'
        WHEN LOWER(TRIM(kontakte.korrespondenzsprache)) = 'englisch' THEN 'EN'
        WHEN LOWER(TRIM(kontakte.korrespondenzsprache)) = 'französisch' THEN 'FR'
        WHEN LOWER(TRIM(kontakte.korrespondenzsprache)) = 'spanisch' THEN 'ES'
        WHEN LOWER(TRIM(kontakte.korrespondenzsprache)) = 'italienisch' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kontakte.kd_nummer) AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte