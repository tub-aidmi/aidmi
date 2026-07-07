{{ config(materialized='table') }}

WITH contacts_source AS (
    SELECT
        kontakt_id,
        rufname,
        familienname,
        kontakt_email,
        tel,
        berufsbezeichnung,
        rolle,
        korrespondenzsprache,
        kd_nummer
    FROM
        {{ source('fixture_master_v2_src', 'master_kontakte') }}
),
accounts_source AS (
    SELECT
        kundennummer,
        -- Generate Account Id here once, as it's needed for AccountId in Contact
        SUBSTRING(MD5(kundennummer), 1, 15) || 'AAA' AS sf_account_id
    FROM
        {{ source('fixture_master_v2_src', 'master_kunden') }}
)
SELECT
    -- Id: Generate 18-char Salesforce Id from kontakt_id
    SUBSTRING(MD5(cs.kontakt_id), 1, 15) || 'AAA' AS "Id",

    -- FirstName: TRIM and INITCAP
    TRIM(INITCAP(cs.rufname)) AS "FirstName",

    -- LastName: TRIM and INITCAP, COALESCE for NOT NULL
    COALESCE(TRIM(INITCAP(cs.familienname)), 'Unknown') AS "LastName",

    -- Email: LOWER and TRIM
    LOWER(TRIM(cs.kontakt_email)) AS "Email",

    -- Phone: TRIM
    TRIM(cs.tel) AS "Phone",

    -- Title: TRIM and INITCAP
    TRIM(INITCAP(cs.berufsbezeichnung)) AS "Title",

    -- Role__c: Map source 'rolle' to enum
    CASE UPPER(TRIM(cs.rolle))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'ENDANWENDER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'TECHNISCHER ANSPRECHPARTNER' THEN 'Technical Contact'
        WHEN 'TECHNIKER' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN 'SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",

    -- Preferred_Language__c: Map source 'korrespondenzsprache' to enum
    CASE UPPER(TRIM(cs.korrespondenzsprache))
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'DE' THEN 'DE'
        WHEN 'GERMAN' THEN 'DE'
        WHEN 'ENGLISH' THEN 'EN'
        WHEN 'EN' THEN 'EN'
        WHEN 'FRENCH' THEN 'FR'
        WHEN 'FR' THEN 'FR'
        WHEN 'FRANZÖSISCH' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",

    -- AccountId: Join with accounts_source to get the generated Account Id
    acs.sf_account_id AS "AccountId",

    -- Legacy_Contact_ID__c: Direct map from kontakt_id
    cs.kontakt_id AS "Legacy_Contact_ID__c",

    -- CreatedDate: Default to CURRENT_TIMESTAMP::text
    CURRENT_TIMESTAMP::text AS "CreatedDate",

    -- LastModifiedDate: Default to CURRENT_TIMESTAMP::text
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",

    -- IsDeleted: Default to 0
    0 AS "IsDeleted"

FROM
    contacts_source cs
LEFT JOIN
    accounts_source acs ON cs.kd_nummer = acs.kundennummer