/*
 * distributed_transaction_management.h
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DISTRIBUTED_TRANSACTION_MANAGEMENT_H_
#define DISTRIBUTED_TRANSACTION_MANAGEMENT_H_


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


extern void InitializeDistributedTransactionManagement(void);
extern void InitializeDistributedTransactionManagementBackend(void);
extern void UnSetDistributedTransactionId(void);
extern DistributedTransactionId * GenerateNextDistributedTransactionId(void);

#endif /* DISTRIBUTED_TRANSACTION_MANAGEMENT_H_ */
