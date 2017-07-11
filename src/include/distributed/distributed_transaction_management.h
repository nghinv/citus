/*
 * distributed_transaction_management.h
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DISTRIBUTED_TRANSACTION_MANAGEMENT_H_
#define DISTRIBUTED_TRANSACTION_MANAGEMENT_H_


#include "datatype/timestamp.h"
#include "nodes/pg_list.h"
#include "storage/s_lock.h"
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
 * Citus identifies a distributed transaction with a triplet consisting of
 *
 *  -  initiatorNodeIdentifier: A unique identifier of the node that initiated
 *     the distributed transaction
 *  -  transactionId: A locally unique identifier assigned for the distributed
 *     transaction on the node that initiated the distributed transaction
 *  -  timestamp: The current timestamp of distributed transaction initiation
 *
 */
typedef struct DistributedTransactionId
{
	uint64 initiatorNodeIdentifier;
	uint64 transactionId;
	TimestampTz timestamp;
} DistributedTransactionId;


/*
 * Each backend's active distributed transaction information is tracked via
 * DistributedTransactionBackendData on the shared memory.
 */
typedef struct DistributedTransactionBackendData
{
	Oid databaseId;
	slock_t mutex;
	DistributedTransactionId transactionId;
} DistributedTransactionBackendData;

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


extern DistributedTransactionShmemData * GetShmem(void);
extern List * GetAllActiveDistributedTransactions(void);
extern Datum GenerateDistributedTransactionIdTuple(Oid databaseId, uint64
												   initiatorNodeIdentifier, uint64
												   transactionId, TimestampTz timestamp);
extern void InitializeDistributedTransactionManagement(void);
extern void InitializeDistributedTransactionManagementBackend(void);
extern void UnSetDistributedTransactionId(void);
extern DistributedTransactionId * GetCurrentDistributedTransctionId(void);
extern void GenerateAndSetNextDistributedTransactionId(void);

#endif /* DISTRIBUTED_TRANSACTION_MANAGEMENT_H_ */
