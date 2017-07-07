--
-- MULTI_DISTRIBUTED_TRANSACTION_ID
-- 
-- Unit tests for distributed transaction id functionality
--

-- get the current transaction id, which should be uninitialized
-- note that we skip printing the databaseId, which might change
-- per run

-- set timezone to a specific value to prevent
-- different values on different servers
SET TIME ZONE 'PST8PDT';

SELECT 
	nodeid, tx, time 
FROM 
	get_distributed_transaction_id() AS f(databaseId oid, nodeId bigint, tx bigint, time timestamptz);


BEGIN;
	
	-- we should still see the uninitialized values
	SELECT 
		nodeid, tx, time 
	FROM 
		get_distributed_transaction_id() AS f(databaseId oid, nodeId bigint, tx bigint, time timestamptz);


	-- now assign a value
    SELECT assign_distributed_transaction_id(50, 50, '2016-01-01 00:00:00+0');

    -- see the assigned value
    SELECT 
		nodeid, tx, time 
	FROM 
		get_distributed_transaction_id() AS f(databaseId oid, nodeId bigint, tx bigint, time timestamptz);

	-- tough not very useful, we can set it again inside a transaction
    SELECT assign_distributed_transaction_id(51, 51, '2017-01-01 00:00:00+0');

    -- see the newly assigned value
    SELECT 
		nodeid, tx, time 
	FROM 
		get_distributed_transaction_id() AS f(databaseId oid, nodeId bigint, tx bigint, time timestamptz);

COMMIT;

-- since the transaction finished, we should see the uninitialized values
SELECT 
	nodeid, tx, time 
FROM 
	get_distributed_transaction_id() AS f(databaseId oid, nodeId bigint, tx bigint, time timestamptz);


-- also see that ROLLBACK (i.e., failures in the transaction) clears the shared memory
BEGIN;
	
	-- we should still see the uninitialized values
	SELECT 
		nodeid, tx, time 
	FROM 
		get_distributed_transaction_id() AS f(databaseId oid, nodeId bigint, tx bigint, time timestamptz);

	-- now assign a value
    SELECT assign_distributed_transaction_id(52, 52, '2015-01-01 00:00:00+0');

    SELECT 5 / 0;
COMMIT;

-- since the transaction errored, we should see the uninitialized values again
SELECT 
	nodeid, tx, time 
FROM 
	get_distributed_transaction_id() AS f(databaseId oid, nodeId bigint, tx bigint, time timestamptz);


-- we should also see that a new connection means an uninitialized transaction ids
BEGIN;
	
	SELECT assign_distributed_transaction_id(52, 52, '2015-01-01 00:00:00+0');

	SELECT 
		nodeid, tx, time 
	FROM 
		get_distributed_transaction_id() AS f(databaseId oid, nodeId bigint, tx bigint, time timestamptz);

	\c - - - :master_port


	SELECT 
		nodeid, tx, time 
	FROM 
		get_distributed_transaction_id() AS f(databaseId oid, nodeId bigint, tx bigint, time timestamptz);

-- set back to the original zone
SET TIME ZONE DEFAULT;
