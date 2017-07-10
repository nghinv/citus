/* citus--7.0-3--7.0-4.sql */

SET search_path = 'pg_catalog';

CREATE TYPE noderole AS ENUM (
	'primary',     -- node is available and accepting writes
	'secondary',   -- node is available but only accepts reads
	'unavailable' -- node is in recovery or otherwise not usable
);

ALTER TABLE pg_dist_node ADD COLUMN noderole noderole NOT NULL DEFAULT 'primary';

DROP FUNCTION master_add_node(text, integer);
CREATE FUNCTION master_add_node(nodename text,
				nodeport integer,
				nodegroup integer default 0,
				noderole noderole default 'primary',
				OUT nodeid integer,
				OUT groupid integer,
				OUT nodename text,
				OUT nodeport integer,
				OUT noderack text,
				OUT hasmetadata boolean,
				OUT isactive bool,
				OUT noderole noderole)
	RETURNS record
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$master_add_node$$;
COMMENT ON FUNCTION master_add_node(nodename text, nodeport integer,
									nodegroup integer, noderole noderole)
	IS 'add node to the cluster';

DROP FUNCTION master_add_inactive_node(text, integer);
CREATE FUNCTION master_add_inactive_node(nodename text,
                                         nodeport integer,
										 nodegroup integer default 0,
										 noderole noderole default 'primary',
										 OUT nodeid integer,
										 OUT groupid integer,
										 OUT nodename text,
										 OUT nodeport integer,
										 OUT noderack text,
										 OUT hasmetadata boolean,
										 OUT isactive bool,
										 OUT noderole noderole)
	RETURNS record
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME',$$master_add_inactive_node$$;
COMMENT ON FUNCTION master_add_inactive_node(nodename text,nodeport integer,
											 nodegroup integer, noderole noderole)
	IS 'prepare node by adding it to pg_dist_node';

RESET search_path;
