SELECT
    asset.id AS "Id",
    COALESCE(asset.name, 'Unknown Asset') AS "Name",
    asset.serial AS "Serial_Number__c",
    asset.warranty AS "Warranty_End_Date__c",
    COALESCE(account_by_id.id, account_by_name.id) AS "Account__c",
    project.id AS "Project__c",
    asset.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    وَsource('fixture_missing_relations_v2_src', 'asset')ğ AS asset
LEFT JOIN
    وَsource('fixture_missing_relations_v2_src', 'account')ğ AS account_by_id
    ON asset.client = account_by_id.id
LEFT JOIN
    وَsource('fixture_missing_relations_v2_src', 'account')ğ AS account_by_name
    ON asset.client = account_by_name.name
LEFT JOIN
    وَsource('fixture_missing_relations_v2_src', 'project')ğ AS project
    ON asset.project = project.id
;