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
#include "funcapi.h"

#include "catalog/pg_type.h"
#include "distributed/distributed_transaction_management.h"
#include "distributed/listutils.h"
#include "distributed/test_helper_functions.h" /* IWYU pragma: keep */
#include "nodes/pg_list.h"
#include "utils/timestamp.h"


static int CompareDistributedTransactionIds(const void *leftElement,
											const void *rightElement);


PG_FUNCTION_INFO_V1(get_all_active_distributed_transaction_ids);


/*
 * get_all_active_distributed_transaction_ids returns all the
 * active distributed transaction ids.
 */
Datum
get_all_active_distributed_transaction_ids(PG_FUNCTION_ARGS)
{
	FuncCallContext *functionContext;

	MemoryContext oldcontext;
	List *activeTransactionList = NULL;
	ListCell *activeTransactionCell = NULL;

	if (SRF_IS_FIRSTCALL())
	{
		functionContext = SRF_FIRSTCALL_INIT();

		/* switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(functionContext->multi_call_memory_ctx);


		activeTransactionList = GetAllActiveDistributedTransactions();
		activeTransactionList = SortList(activeTransactionList,
										 CompareDistributedTransactionIds);


		activeTransactionCell = list_head(activeTransactionList);

		functionContext->user_fctx = activeTransactionCell;


		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	functionContext = SRF_PERCALL_SETUP();

	activeTransactionCell = (ListCell *) functionContext->user_fctx;
	if (activeTransactionCell != NULL)
	{
		DistributedTransactionBackendData *backendData =
			(DistributedTransactionBackendData *) lfirst(activeTransactionCell);

		Datum datumVal = GenerateDistributedTransactionIdTuple(backendData->databaseId,
															   backendData->transactionId.
															   initiatorNodeIdentifier,
															   backendData->transactionId.
															   transactionId,
															   backendData->transactionId.
															   timestamp);

		functionContext->user_fctx = lnext(activeTransactionCell);

		SRF_RETURN_NEXT(functionContext, datumVal);
	}
	else
	{
		SRF_RETURN_DONE(functionContext);
	}
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
