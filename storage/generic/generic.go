package generic

var (
	RevisionSQL = `
	SELECT MAX(events.revision) AS current_revision
	FROM watchrelay AS events`
	Columns = `
	log.revision, log.create_revision, log.resource_name, log.created, log.deleted, log.value, log.created_at`
	FillGapSQL = `
	INSERT INTO watchrelay(revision, resource_name, created, deleted, create_revision, prev_revision, value, created_at)
	values(?, ?, 1, 1, ?, 0, "", ?)`
)
