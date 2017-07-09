/*
 * distributed_transaction_management.h
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DISTRIBUTED_TRANSACTION_MANAGEMENT_H_
#define DISTRIBUTED_TRANSACTION_MANAGEMENT_H_

extern void InitializeDistributedTransactionManagement(void);
extern void InitializeDistributedTransactionManagementBackend(void);
extern void UnSetDistributedTransactionId(void);
extern uint64 GetNextDistributedTransactionId(void);

#endif /* DISTRIBUTED_TRANSACTION_MANAGEMENT_H_ */
