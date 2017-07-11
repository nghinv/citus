/* citus--7.0-3--7.0-4.sql */

SET search_path = 'pg_catalog';

CREATE TYPE noderole AS ENUM (
	'primary',     -- node is available and accepting writes
	'secondary',   -- node is available but only accepts reads
	'unavailable' -- node is in recovery or otherwise not usable
);

ALTER TABLE pg_dist_node ADD COLUMN noderole noderole NOT NULL DEFAULT 'primary';

-- we're now allowed to have more than one node per group
ALTER TABLE pg_catalog.pg_dist_node DROP CONSTRAINT pg_dist_node_groupid_unique;

CREATE OR REPLACE FUNCTION citus.pg_dist_node_trigger_func()
RETURNS TRIGGER AS $$
  BEGIN
    IF (TG_OP = 'INSERT') THEN
	IF NEW.noderole = 'primary'
	   AND EXISTS (SELECT 1 FROM pg_dist_node WHERE groupid = NEW.groupid AND
		                                        noderole = 'primary' AND
						        nodeid <> NEW.nodeid) THEN
	  RAISE EXCEPTION 'there cannot be two primary nodes in a group';
	END IF;
	RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
	IF NEW.noderole = 'primary'
	   AND EXISTS (SELECT 1 FROM pg_dist_node WHERE groupid = NEW.groupid AND
		                                        noderole = 'primary' AND
						        nodeid <> NEW.nodeid) THEN
	  RAISE EXCEPTION 'there cannot be two primary nodes in a group';
	END IF;
	RETURN NEW;
    END IF;
  END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER pg_dist_node_trigger
  BEFORE INSERT OR UPDATE ON pg_dist_node
  FOR EACH ROW EXECUTE PROCEDURE citus.pg_dist_node_trigger_func();

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
