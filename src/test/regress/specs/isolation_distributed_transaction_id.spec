# Tests around cancelling statements. As we can't trigger cancel
# interrupts directly, we use statement_timeout instead, which largely
# behaves the same as proper cancellation.

setup
{
	CREATE OR REPLACE FUNCTION print_all_active_distributed_transaction_ids()
	RETURNS void
	LANGUAGE C STRICT
	AS 'citus',$$print_all_active_distributed_transaction_ids$$;
 	COMMENT ON FUNCTION print_all_active_distributed_transaction_ids()
	IS 'prints all the distributed transaction ids';

	SET TIME ZONE 'PST8PDT';
}

teardown
{
    DROP FUNCTION print_all_active_distributed_transaction_ids();
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

step "s4-print-all-transactions"
{
	SELECT print_all_active_distributed_transaction_ids();
}

# show that we could get all distributed transaction ids from seperate sessions
permutation "s1-begin" "s1-assign-transaction-id" "s4-print-all-transactions" "s2-begin" "s2-assign-transaction-id" "s4-print-all-transactions" "s3-begin" "s3-assign-transaction-id" "s4-print-all-transactions" "s1-commit" "s4-print-all-transactions" "s2-commit" "s4-print-all-transactions" "s3-commit" "s4-print-all-transactions"   



