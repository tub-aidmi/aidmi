{{ config(materialized='table') }}

SELECT
    '001' || LPAD(REPLACE(TRIM(kunden_nr), 'CUST-', ''), 8, '0') AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE LOWER(TRIM(kategorie))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silber' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(gebiet)) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    TRIM(webseite) AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    CASE UPPER(TRIM(land))
        WHEN 'ALANDINSELN' THEN 'AX'
        WHEN 'ALGERIEN' THEN 'DZ'
        WHEN 'AMERIKANISCH-OZEANIEN' THEN 'AS'
        WHEN 'AMERIKANISCHE JUNGFERNINSELN' THEN 'VI'
        WHEN 'ANGOLA' THEN 'AO'
        WHEN 'ANGUILLA' THEN 'AI'
        WHEN 'ANTARKTIS' THEN 'AQ'
        WHEN 'ARGENTINIEN' THEN 'AR'
        WHEN 'ARMENIEN' THEN 'AM'
        WHEN 'ARUBA' THEN 'AW'
        WHEN 'ASERBAIDSCHAN' THEN 'AZ'
        WHEN 'BANGLADESCH' THEN 'BD'
        WHEN 'BARBADOS' THEN 'BB'
        WHEN 'BOSNIEN UND HERZEGOWINA' THEN 'BA'
        WHEN 'BRASILIEN' THEN 'BR'
        WHEN 'BRITISCHES TERRITORIUM IM INDISCHEN OZEAN' THEN 'IO'
        WHEN 'BULGARIEN' THEN 'BG'
        WHEN 'DSCHIBUTI' THEN 'DJ'
        WHEN 'ECUADOR' THEN 'EC'
        WHEN 'FIDSCHI' THEN 'FJ'
        WHEN 'FRANZÖSISCH-GUYANA' THEN 'GF'
        WHEN 'FRANZÖSISCH-POLYNESIEN' THEN 'PF'
        WHEN 'FÄRÖER' THEN 'FO'
        WHEN 'GIBRALTAR' THEN 'GI'
        WHEN 'GRENADA' THEN 'GD'
        WHEN 'GUADELOUPE' THEN 'GP'
        WHEN 'GUAM' THEN 'GU'
        WHEN 'GUERNSEY' THEN 'GG'
        WHEN 'GUINEA' THEN 'GN'
        WHEN 'GUYANA' THEN 'GY'
        WHEN 'HAITI' THEN 'HT'
        WHEN 'HONDURAS' THEN 'HN'
        WHEN 'IRAK' THEN 'IQ'
        WHEN 'ISLAND' THEN 'IS'
        WHEN 'ITALIEN' THEN 'IT'
        WHEN 'JERSEY' THEN 'JE'
        WHEN 'KAIMANINSELN' THEN 'KY'
        WHEN 'KANADA' THEN 'CA'
        WHEN 'KAP VERDE' THEN 'CV'
        WHEN 'KONGO' THEN 'CG'
        WHEN 'KROATIEN' THEN 'HR'
        WHEN 'LESOTHO' THEN 'LS'
        WHEN 'LETTLAND' THEN 'LV'
        WHEN 'MALAYSIA' THEN 'MY'
        WHEN 'MALI' THEN 'ML'
        WHEN 'MARTINIQUE' THEN 'MQ'
        WHEN 'MAURETANIEN' THEN 'MR'
        WHEN 'MAURITIUS' THEN 'MU'
        WHEN 'MEKIKO' THEN 'MX'
        WHEN 'MONGOLEI' THEN 'MN'
        WHEN 'MOSAMBIK' THEN 'MZ'
        WHEN 'NAURU' THEN 'NR'
        WHEN 'NIEDERLANDE' THEN 'NL'
        WHEN 'NIGER' THEN 'NE'
        WHEN 'NORDMAZEDONIEN' THEN 'MK'
        WHEN 'PALAU' THEN 'PW'
        WHEN 'PAPUA-NEUGUINEA' THEN 'PG'
        WHEN 'PARAGUAY' THEN 'PY'
        WHEN 'PERU' THEN 'PE'
        WHEN 'PUERTO RICO' THEN 'PR'
        WHEN 'RUANDA' THEN 'RW'
        WHEN 'RUMANIEN' THEN 'RO'
        WHEN 'SALOMONEN' THEN 'SB'
        WHEN 'ZAMBIA' THEN 'ZM'
        WHEN 'SAMOA' THEN 'WS'
        WHEN 'SCHWEDEN' THEN 'SE'
        WHEN 'SERBIEN' THEN 'RS'
        WHEN 'SIERRA LEONE' THEN 'SL'
        WHEN 'SIMBABWE' THEN 'ZW'
        WHEN 'SINGAPUR' THEN 'SG'
        WHEN 'ST. HELENA' THEN 'SH'
        WHEN 'ST. PIERRE UND MIQUELON' THEN 'PM'
        WHEN 'SVALBARD UND JAN MAYEN' THEN 'SJ'
        WHEN 'SWASILAND' THEN 'SZ'
        WHEN 'SÜDAFRIKA' THEN 'ZA'
        WHEN 'TAIWAN' THEN 'TW'
        WHEN 'THAILAND' THEN 'TH'
        WHEN 'TOGO' THEN 'TG'
        WHEN 'TOKELAU' THEN 'TK'
        WHEN 'USBEKISTAN' THEN 'UZ'
        WHEN 'VATIKANSTADT' THEN 'VA'
        WHEN 'VEREINIGTES KÖNIGREICH' THEN 'GB'
        WHEN 'WALLIS UND FUTUNA' THEN 'WF'
        WHEN 'WEIHNACHTSINSEL' THEN 'CX'
        WHEN 'ÄGYPTEN' THEN 'EG'
        WHEN 'ÄQUATORIALGUINEA' THEN 'GQ'
        ELSE NULL
    END AS "BillingCountry",
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}