/*-------------------------------------------------------------------------
 *
 * test/src/distributed_transaction_id.c
 *
 * This file contains functions to exercise distributed transaction id
 * functionality within Citus.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include "catalog/pg_type.h"
#include "distributed/distributed_transaction_management.h"
#include "distributed/listutils.h"
#include "distributed/test_helper_functions.h" /* IWYU pragma: keep */
#include "nodes/pg_list.h"
#include "utils/timestamp.h"


static int CompareDistributedTransactionIds(const void *leftElement,
											const void *rightElement);


PG_FUNCTION_INFO_V1(print_all_active_distributed_transaction_ids);


/*
 * prune_using_no_values returns the shards for the specified distributed table
 * after pruning using an empty clause list.
 */
Datum
print_all_active_distributed_transaction_ids(PG_FUNCTION_ARGS)
{
	List *activeTransactions = GetAllActiveDistributedTransactions();
	ListCell *activeTransactionCell = NULL;

	/* sort for consistent outputs */
	activeTransactions = SortList(activeTransactions, CompareDistributedTransactionIds);

	foreach(activeTransactionCell, activeTransactions)
	{
		DistributedTransactionBackendData *backendData =
			(DistributedTransactionBackendData *) lfirst(activeTransactionCell);

		elog(INFO, "(%d, %ld, %ld, '%s')", backendData->databaseId,
			 backendData->transactionId.initiatorNodeIdentifier,
			 backendData->transactionId.transactionId,
			 timestamptz_to_str(backendData->transactionId.timestamp));
	}

	PG_RETURN_VOID();
}


/*
 * CompareDistributedTransactionIds is a test function, which only sorts elements
 * wrt transactionId field only.
 */
static int
CompareDistributedTransactionIds(const void *leftElement, const void *rightElement)
{
	const DistributedTransactionBackendData *leftBackendData =
		*((const DistributedTransactionBackendData **) leftElement);
	const DistributedTransactionBackendData *rightBackendData =
		*((const DistributedTransactionBackendData **) rightElement);

	uint64 leftTransactionId = leftBackendData->transactionId.transactionId;
	uint64 rightTransactionId = rightBackendData->transactionId.transactionId;

	if (leftTransactionId < rightTransactionId)
	{
		return -1;
	}
	else if (leftTransactionId > rightTransactionId)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}
