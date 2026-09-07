-- A workflow is an ordinary durable job. Only its current ownership link needs
-- extra storage; stage identities remain in the existing change jobs document.
ALTER TABLE changes ADD COLUMN workflow_job_id TEXT REFERENCES jobs(id);
