setup
{
	SELECT 1 FROM master_add_node('localhost', 57637);
	SELECT * FROM master_get_active_worker_nodes() ORDER BY node_name, node_port;
}

teardown
{
	DROP TABLE IF EXISTS ref_table;

	SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-add-node-2"
{
	SELECT 1 FROM master_add_node('localhost', 57638);
}

step "s1-remove-node-1"
{
	SELECT 1 FROM master_remove_node('localhost', 57637);
}

step "s1-remove-node-2"
{
	SELECT * FROM master_remove_node('localhost', 57638);
}

step "s1-abort"
{
	ABORT;
}

step "s1-commit"
{
	COMMIT;
}

step "s1-query-table"
{
	SELECT * FROM ref_table;
}

step "s1-show-nodes"
{
	SELECT nodename, nodeport, isactive FROM pg_dist_node ORDER BY nodename, nodeport;
}

step "s1-show-placements"
{
	SELECT
		nodename, nodeport
	FROM
		pg_dist_shard_placement JOIN pg_dist_shard USING (shardid)
	WHERE
		logicalrelid = 'ref_table'::regclass
	ORDER BY
		nodename, nodeport;
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-create-ref"
{
	CREATE TABLE ref_table (x int, y int);
	SELECT create_reference_table('ref_table');
}

step "s2-insert"
{
	INSERT INTO ref_table VALUES (1,2);
}

step "s2-select"
{
	SELECT * FROM ref_table;
}

step "s2-commit"
{
	COMMIT;
}

# session 1 adds a node, session 2 creates a reference table
permutation "s1-begin" "s1-add-node-2" "s2-create-ref" "s1-commit" "s1-show-placements"
permutation "s1-begin" "s1-add-node-2" "s2-create-ref" "s1-abort" "s1-show-placements"
permutation "s2-begin" "s2-create-ref" "s1-add-node-2" "s2-commit" "s1-show-placements"

# session 1 removes a node, session 2 creates a reference table
permutation "s1-add-node-2" "s1-begin" "s1-remove-node-2" "s2-create-ref" "s1-commit" "s1-show-placements"
permutation "s1-add-node-2" "s1-begin" "s1-remove-node-2" "s2-create-ref" "s1-abort" "s1-show-placements"
permutation "s1-add-node-2" "s2-begin" "s2-create-ref" "s1-remove-node-2" "s2-commit" "s1-show-placements"

# session 1 adds a node, session 2 insert into a reference table
permutation "s2-create-ref" "s1-begin" "s1-add-node-2" "s2-insert" "s1-commit" "s2-select" "s1-remove-node-1" "s2-select"
permutation "s2-create-ref" "s2-begin" "s2-insert" "s1-add-node-2" "s2-commit" "s2-select" "s1-remove-node-1" "s2-select"

# session 1 removes a node, session 2 inserts into a reference table
permutation "s1-add-node-2" "s2-create-ref" "s1-begin" "s1-remove-node-2" "s2-insert" "s1-commit" "s2-select"
permutation "s1-add-node-2" "s2-create-ref" "s2-begin" "s2-insert" "s1-remove-node-2" "s2-commit" "s2-select"
