{{ config(materialized='table') }}

SELECT
    k."kontakt_id" AS "Id",
    INITCAP(TRIM(k."rufname")) AS "FirstName",
    COALESCE(NULLIF(TRIM(k."familienname"), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(k."kontakt_email")) AS "Email",
    TRIM(k."tel") AS "Phone",
    INITCAP(TRIM(k."berufsbezeichnung")) AS "Title",
    CASE UPPER(TRIM(COALESCE(k."rolle", '')))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'ENDANWENDER' THEN 'End User'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN 'SPONSOR' THEN 'Executive Sponsor'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'TECHNIKER' THEN 'Technical Contact'
        WHEN 'TECHNISCHER ANSPRECHPARTNER' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(COALESCE(k."korrespondenzsprache", '')))
        WHEN 'DE' THEN 'DE'
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'ENGLISH' THEN 'EN'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'FRENCH' THEN 'FR'
        WHEN 'FRANZÖSISCH' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    k."kd_nummer" AS "AccountId",
    k."kontakt_id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k