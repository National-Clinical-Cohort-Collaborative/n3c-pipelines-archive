CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/IssueLabelsForReleaseNotes/PersistentIssues` AS

    --pull the list of issues : "Disposition: Persistent: Not a fixable problem"
    SELECT DISTINCT * 
    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/Issues Dashboard/issues datasets/issues` issues
    INNER JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/Issues Dashboard/issues datasets/issue_tag` tags
        ON issues.rid = tags.issue_rid
        AND tags.tag_rid = 'ri.issues.main.tag.bdf1ff83-9bee-4fc6-9ec5-7ea6bc428deb' -- rid for "Disposition: Persistent: Not a fixable problem"
    WHERE issues.archived = false AND issues.status <> 'CLOSED'
    