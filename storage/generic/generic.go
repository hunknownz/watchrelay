package generic

var (
	RevisionSQL = `
	SELECT MAX(events.revision) AS current_revision
	FROM watchrelay AS events`
	Columns = `
	log.revision, log.create_revision, log.resource_name, log.created, log.deleted, log.value, log.created_at`
)
