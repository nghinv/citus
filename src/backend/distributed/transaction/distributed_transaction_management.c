/*-------------------------------------------------------------------------
 *
 * distributed_transaction_management.c
 *
 *  Infrastructure for distributed transaction management and distributed deadlock
 *  detection.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "distributed/distributed_transaction_management.h"
#include "distributed/metadata_cache.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/spin.h"
#include "storage/s_lock.h"
#include "utils/timestamp.h"


/*
 * Each backend's active distributed transaction data reside in the
 * shared memory on the DistributedTransactionShmemData.
 */
typedef struct DistributedTransactionShmemData
{
	int trancheId;
#if (PG_VERSION_NUM >= 100000)
	NamedLWLockTranche namedLockTranche;
#else
	LWLockTranche lockTranche;
#endif
	LWLock lock;

	/*
	 * We prefer to use an atomic integer over sequences for two
	 * reasons (i) orders of magnitude performance difference
	 * (ii) allowing read-only replicas to be able to generate ids
	 */
	pg_atomic_uint64 nextTransactionId;

	DistributedTransactionBackendData sessions[FLEXIBLE_ARRAY_MEMBER];
} DistributedTransactionShmemData;


static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static DistributedTransactionShmemData *distributedTransactionShmemData = NULL;
static DistributedTransactionBackendData *MyDistributedTransactionBackend = NULL;


static void DistributedTransactionManagementShmemInit(void);
static TupleDesc GenerateDistributedTransactionTupleDesc(void);
static size_t DistributedTransactionManagementShmemSize(void);
static DistributedTransactionId * GenerateNextDistributedTransactionId(void);
static uint64 GetNextLocalTransactionIdFromShmem(void);


PG_FUNCTION_INFO_V1(assign_distributed_transaction_id);
PG_FUNCTION_INFO_V1(get_distributed_transaction_id);


/*
 * assign_distributed_transaction_id updates the shared memory allocated for this backend
 * and sets initiatorNodeIdentifier, transactionId, timestamp fields with the given
 * inputs. Also, the function sets the database id via MyDatabaseId variable provided
 * by "misadmmin.h".
 */
Datum
assign_distributed_transaction_id(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	/* MyDistributedTransactionBackend should always be avaliable, just out of paranoia */
	if (!MyDistributedTransactionBackend)
	{
		ereport(ERROR, (errmsg("backend is not ready for distributed transactions")));
	}

	SpinLockAcquire(&MyDistributedTransactionBackend->mutex);

	MyDistributedTransactionBackend->databaseId = MyDatabaseId;

	MyDistributedTransactionBackend->transactionId.initiatorNodeIdentifier =
		PG_GETARG_INT64(0);
	MyDistributedTransactionBackend->transactionId.transactionId = PG_GETARG_INT64(1);
	MyDistributedTransactionBackend->transactionId.timestamp = PG_GETARG_TIMESTAMPTZ(2);

	SpinLockRelease(&MyDistributedTransactionBackend->mutex);

	PG_RETURN_VOID();
}


/*
 * get_distributed_transaction_id returns a tuple with (databaseId, initiatorNodeIdentifier,
 * transactionId, timestamp) that exists in the shared memory associated with this backend.
 */
Datum
get_distributed_transaction_id(PG_FUNCTION_ARGS)
{
	Datum distributedTransactionId = 0;

	Oid databaseId = InvalidOid;
	uint64 initiatorNodeIdentifier = 0;
	uint64 transactionId = 0;
	TimestampTz timestamp = 0;

	CheckCitusVersion(ERROR);

	/* MyDistributedTransactionBackend should always be avaliable, just out of paranoia */
	if (!MyDistributedTransactionBackend)
	{
		ereport(ERROR, (errmsg("backend is not ready for distributed transactions")));
	}

	/* read the shmem while mutex is held */
	SpinLockAcquire(&MyDistributedTransactionBackend->mutex);

	databaseId = MyDistributedTransactionBackend->databaseId;
	initiatorNodeIdentifier =
		MyDistributedTransactionBackend->transactionId.initiatorNodeIdentifier;
	transactionId = MyDistributedTransactionBackend->transactionId.transactionId;
	timestamp = MyDistributedTransactionBackend->transactionId.timestamp;

	SpinLockRelease(&MyDistributedTransactionBackend->mutex);

	distributedTransactionId =
		GenerateDistributedTransactionIdTuple(databaseId, initiatorNodeIdentifier,
											  transactionId, timestamp);

	PG_RETURN_DATUM(distributedTransactionId);
}


/*
 * GetAllActiveDistributedTransactions returns all the active distributed
 * transactions on the shared memory.
 *
 * Note that the function could not gurantee a global consistent result, the function
 * only locks each active backend while copying it. Currently the only caller of this
 * function is in the src/test folder. The consistency gurantee is sufficient for
 * testing.
 */
List *
GetAllActiveDistributedTransactions(void)
{
	int sessionIndex = 0;
	List *activeTransactionList = NIL;

	for (sessionIndex = 0; sessionIndex < MaxBackends; ++sessionIndex)
	{
		DistributedTransactionBackendData *currentBackend =
			&distributedTransactionShmemData->sessions[sessionIndex];

		if (currentBackend && currentBackend->transactionId.transactionId != 0)
		{
			DistributedTransactionBackendData *activeBackend =
				(DistributedTransactionBackendData *) palloc(
					sizeof(DistributedTransactionBackendData));

			SpinLockAcquire(&MyDistributedTransactionBackend->mutex);

			activeBackend->databaseId = currentBackend->databaseId;
			activeBackend->transactionId.initiatorNodeIdentifier =
				currentBackend->transactionId.initiatorNodeIdentifier;
			activeBackend->transactionId.transactionId =
				currentBackend->transactionId.transactionId;
			activeBackend->transactionId.timestamp =
				currentBackend->transactionId.timestamp;

			SpinLockRelease(&MyDistributedTransactionBackend->mutex);

			activeTransactionList = lappend(activeTransactionList, currentBackend);
		}
	}

	return activeTransactionList;
}


/*
 * GenerateDistributedTransactionIdTuple returns a datum of pseudo-generated heaptuple
 * for the distributed transaction id provided by the parameters.
 */
Datum
GenerateDistributedTransactionIdTuple(Oid databaseId, uint64 initiatorNodeIdentifier,
									  uint64 transactionId, TimestampTz timestamp)
{
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum distributedTransactionIdDatum = 0;
	int attributeCount = 4;
	Datum values[attributeCount];
	bool isNulls[attributeCount];

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[0] = ObjectIdGetDatum(databaseId);
	values[1] = UInt64GetDatum(initiatorNodeIdentifier);
	values[2] = UInt64GetDatum(transactionId);
	values[3] = TimestampTzGetDatum(timestamp);

	tupleDescriptor = GenerateDistributedTransactionTupleDesc();

	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);
	distributedTransactionIdDatum = HeapTupleGetDatum(heapTuple);

	return distributedTransactionIdDatum;
}


/*
 * GenerateDistributedTransactionTupleDesc returns a tuple descriptor
 * for distributed transaction id in the form of (databaseId,
 * initiatorNodeIdentifier, transactionId, timestamp).
 */
static TupleDesc
GenerateDistributedTransactionTupleDesc(void)
{
	TupleDesc distributedTransactionTupleDesc = NULL;
	int attributeCount = 4;
	bool hasOids = false;

	/* generate a custom tuple descriptor for distributed transaction id*/
	distributedTransactionTupleDesc = CreateTemplateTupleDesc(attributeCount, hasOids);

	TupleDescInitEntry(distributedTransactionTupleDesc, (AttrNumber) 1, "databaseId",
					   OIDOID, -1, 0);
	TupleDescInitEntry(distributedTransactionTupleDesc, (AttrNumber) 2,
					   "initiatorNodeIdentifier", INT8OID, -1, 0);
	TupleDescInitEntry(distributedTransactionTupleDesc, (AttrNumber) 3, "transactionId",
					   INT8OID, -1, 0);
	TupleDescInitEntry(distributedTransactionTupleDesc, (AttrNumber) 4, "timestamp",
					   TIMESTAMPTZOID, -1, 0);

	/* let Postgres learn about this tuple descriptor */
	BlessTupleDesc(distributedTransactionTupleDesc);

	return distributedTransactionTupleDesc;
}


/*
 * InitializeDistributedTransactionManagement requests the necessary shared memory
 * from Postgres and sets up the shared memory startup hook.
 */
void
InitializeDistributedTransactionManagement(void)
{
	/* allocate shared memory */
	RequestAddinShmemSpace(DistributedTransactionManagementShmemSize());

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = DistributedTransactionManagementShmemInit;
}


/*
 * DistributedTransactionManagementShmemInit is the callback that is to be called on
 * shared memory startup hook. The function sets up the necessary shared memory segment
 * for the distributed transaction manager.
 */
static void
DistributedTransactionManagementShmemInit(void)
{
	bool alreadyInitialized = false;
	char *trancheName = "Distributed Transaction Management";

	/* we may update the shmem, acquire lock exclusively */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	distributedTransactionShmemData =
		(DistributedTransactionShmemData *) ShmemInitStruct(
			"Distributed Transaction Management",
			DistributedTransactionManagementShmemSize(),
			&alreadyInitialized);

	if (!alreadyInitialized)
	{
#if (PG_VERSION_NUM >= 100000)
		NamedLWLockTranche *namedLockTranche =
			&distributedTransactionShmemData->namedLockTranche;
#else
		LWLockTranche *lockTranche = &distributedTransactionShmemData->lockTranche;
#endif

		/* start by zeroing out all the memory */
		memset(distributedTransactionShmemData, 0,
			   DistributedTransactionManagementShmemSize());

#if (PG_VERSION_NUM >= 100000)
		namedLockTranche->trancheId = LWLockNewTrancheId();

		LWLockRegisterTranche(namedLockTranche->trancheId, trancheName);
		LWLockInitialize(&distributedTransactionShmemData->lock,
						 namedLockTranche->trancheId);
#else
		distributedTransactionShmemData->trancheId = LWLockNewTrancheId();

		/* we only need a single lock */
		lockTranche->array_base = &distributedTransactionShmemData->lock;
		lockTranche->array_stride = sizeof(LWLock);
		lockTranche->name = trancheName;

		LWLockRegisterTranche(distributedTransactionShmemData->trancheId, lockTranche);
		LWLockInitialize(&distributedTransactionShmemData->lock,
						 distributedTransactionShmemData->trancheId);
#endif

		/* start the distributed transaction ids from 1 */
		pg_atomic_init_u64(&distributedTransactionShmemData->nextTransactionId, 1);
	}

	LWLockRelease(AddinShmemInitLock);

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * DistributedTransactionShmemSize returns the size that should be allocated
 * on the shared memory for distributed transaction management.
 */
static size_t
DistributedTransactionManagementShmemSize(void)
{
	Size size = 0;

	size = add_size(size, sizeof(DistributedTransactionShmemData));
	size = add_size(size, mul_size(sizeof(DistributedTransactionBackendData),
								   MaxBackends));

	return size;
}


/*
 *  InitializeDistributedTransactionManagementBackend is called per backend
 *  and does the required initilizations.
 */
void
InitializeDistributedTransactionManagementBackend(void)
{
	LWLockAcquire(&distributedTransactionShmemData->lock, LW_EXCLUSIVE);

	MyDistributedTransactionBackend =
		&distributedTransactionShmemData->sessions[MyProc->pgprocno];

	Assert(MyDistributedTransactionBackend);

	MyDistributedTransactionBackend->databaseId = 0;
	MyDistributedTransactionBackend->transactionId.initiatorNodeIdentifier = 0;
	MyDistributedTransactionBackend->transactionId.transactionId = 0;
	MyDistributedTransactionBackend->transactionId.timestamp = 0;

	/* we'll use mutex while accessing the backend shmem */
	SpinLockInit(&MyDistributedTransactionBackend->mutex);

	LWLockRelease(&distributedTransactionShmemData->lock);
}


/*
 * UnSetDistributedTransactionId simply acquires the mutex and resets
 * the backend's distributed transaction data in shared memory to the
 * initial values.
 */
void
UnSetDistributedTransactionId(void)
{
	if (MyDistributedTransactionBackend)
	{
		SpinLockAcquire(&MyDistributedTransactionBackend->mutex);

		MyDistributedTransactionBackend->databaseId = 0;
		MyDistributedTransactionBackend->transactionId.initiatorNodeIdentifier = 0;
		MyDistributedTransactionBackend->transactionId.transactionId = 0;
		MyDistributedTransactionBackend->transactionId.timestamp = 0;

		SpinLockRelease(&MyDistributedTransactionBackend->mutex);
	}
}


/*
 * GetCurrentDistributedTransctionId reads the backend's distributed transaction id and
 * returns a copy of it.
 */
DistributedTransactionId *
GetCurrentDistributedTransctionId(void)
{
	DistributedTransactionId *currentDistributedTransactionId =
		(DistributedTransactionId *) palloc(sizeof(DistributedTransactionId));

	SpinLockAcquire(&MyDistributedTransactionBackend->mutex);

	currentDistributedTransactionId->initiatorNodeIdentifier =
		MyDistributedTransactionBackend->transactionId.initiatorNodeIdentifier;
	currentDistributedTransactionId->transactionId =
		MyDistributedTransactionBackend->transactionId.transactionId;
	currentDistributedTransactionId->timestamp =
		MyDistributedTransactionBackend->transactionId.timestamp;

	SpinLockRelease(&MyDistributedTransactionBackend->mutex);

	return currentDistributedTransactionId;
}


/*
 * GenerateAndSetNextDistributedTransactionId generates a new distributed transaction
 * id and sets it for the current backend.
 *
 * This function should only be called on BeginCoordinatedTransaction(). Any other
 * callers is very likely to break the distributed transction management.
 */
void
GenerateAndSetNextDistributedTransactionId(void)
{
	DistributedTransactionId *distributedTransactionId =
		GenerateNextDistributedTransactionId();

	SpinLockAcquire(&MyDistributedTransactionBackend->mutex);

	MyDistributedTransactionBackend->databaseId = MyDatabaseId;

	MyDistributedTransactionBackend->transactionId.initiatorNodeIdentifier =
		distributedTransactionId->initiatorNodeIdentifier;
	MyDistributedTransactionBackend->transactionId.transactionId =
		distributedTransactionId->transactionId;
	MyDistributedTransactionBackend->transactionId.timestamp =
		distributedTransactionId->timestamp;

	SpinLockRelease(&MyDistributedTransactionBackend->mutex);
}


/*
 * GenerateNextDistributedTransactionId returns a new distributed transaction id with
 * the current timestamp and next local transaction id.
 */
static DistributedTransactionId *
GenerateNextDistributedTransactionId(void)
{
	DistributedTransactionId *nextDistributedTransactionId =
		(DistributedTransactionId *) palloc(sizeof(DistributedTransactionId));

	/*
	 * FIXME: Once we allow running queries from secondaries, we should generate
	 * an id which is unique to the node, not to the group as we do here.
	 */
	nextDistributedTransactionId->initiatorNodeIdentifier = (uint64) GetLocalGroupId();

	nextDistributedTransactionId->transactionId = GetNextLocalTransactionIdFromShmem();
	nextDistributedTransactionId->timestamp = GetCurrentTimestamp();

	return nextDistributedTransactionId;
}


/*
 * GetNextLocalTransactionIdFromShmem atomically fetches and returns
 * the next distributed transaction id from the shared memory.
 */
static uint64
GetNextLocalTransactionIdFromShmem(void)
{
	pg_atomic_uint64 *transactionIdSequence =
		&distributedTransactionShmemData->nextTransactionId;

	return pg_atomic_fetch_add_u64(transactionIdSequence, 1);
}
