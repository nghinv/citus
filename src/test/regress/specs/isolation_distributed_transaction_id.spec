# Tests around cancelling statements. As we can't trigger cancel
# interrupts directly, we use statement_timeout instead, which largely
# behaves the same as proper cancellation.

setup
{
	CREATE OR REPLACE FUNCTION get_all_active_distributed_transaction_ids()
	RETURNS setof record
	LANGUAGE C STRICT
	AS 'citus', $$get_all_active_distributed_transaction_ids$$;

	SET TIME ZONE 'PST8PDT';
}

teardown
{
    DROP FUNCTION get_all_active_distributed_transaction_ids();
	SET TIME ZONE DEFAULT;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-assign-transaction-id"
{
    SELECT assign_distributed_transaction_id(1, 1, '2015-01-01 00:00:00+0');
}

step "s1-commit"
{
    COMMIT;
}

step "s1-create-table-and-function"
{
	-- some tests also use distributed table
	CREATE TABLE distributed_transaction_id_table(some_value int, other_value int);
	SET citus.shard_count TO 4;
	SELECT create_distributed_table('distributed_transaction_id_table', 'some_value');

	SELECT run_command_on_workers($f$
	CREATE OR REPLACE FUNCTION get_all_active_distributed_transaction_ids()
	RETURNS setof record
	LANGUAGE C STRICT
	AS 'citus', $$get_all_active_distributed_transaction_ids$$;
	$f$);
}

step "s1-insert"
{
	INSERT INTO distributed_transaction_id_table VALUES (1, 1);
}

step "s1-get-current-transaction-id"
{
	SELECT row(nodeid, tx) FROM  get_distributed_transaction_id() AS f(databaseId oid, nodeId bigint, tx bigint, time timestamptz);
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-assign-transaction-id"
{
    SELECT assign_distributed_transaction_id(2, 2, '2015-01-02 00:00:00+0');
}

step "s2-commit"
{
    COMMIT;
}

# print only the necessary parts to prevent concurrent runs to print different values
step "s2-get-first-worker-active-transactions"
{
		SELECT * FROM run_command_on_workers('SELECT row(nodeid, tx)
												FROM	 
											  get_all_active_distributed_transaction_ids() AS f(databaseId oid, nodeId bigint, tx bigint, time timestamptz)
											') 
		WHERE nodeport = 57637;
;
}

session "s3"

step "s3-begin"
{
    BEGIN;
}

step "s3-assign-transaction-id"
{
    SELECT assign_distributed_transaction_id(3, 3, '2015-01-03 00:00:00+0');
}

step "s3-commit"
{
    COMMIT;
}

session "s4"

step "s4-get-all-transactions"
{
	SELECT get_all_active_distributed_transaction_ids();
}

# show that we could get all distributed transaction ids from seperate sessions
permutation "s1-begin" "s1-assign-transaction-id" "s4-get-all-transactions" "s2-begin" "s2-assign-transaction-id" "s4-get-all-transactions" "s3-begin" "s3-assign-transaction-id" "s4-get-all-transactions" "s1-commit" "s4-get-all-transactions" "s2-commit" "s4-get-all-transactions" "s3-commit" "s4-get-all-transactions"


# now show that distributed transaction id on the coordinator
# is the same with the one on the worker
permutation "s1-create-table-and-function" "s1-begin" "s1-insert" "s1-get-current-transaction-id" "s2-get-first-worker-active-transactions"

