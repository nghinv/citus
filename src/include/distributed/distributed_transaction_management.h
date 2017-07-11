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


extern List * GetAllActiveDistributedTransactions(void);
extern void InitializeDistributedTransactionManagement(void);
extern void InitializeDistributedTransactionManagementBackend(void);
extern void UnSetDistributedTransactionId(void);
extern DistributedTransactionId * GetCurrentDistributedTransctionId(void);
extern void GenerateAndSetNextDistributedTransactionId(void);

#endif /* DISTRIBUTED_TRANSACTION_MANAGEMENT_H_ */
