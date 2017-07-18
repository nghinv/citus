/*-------------------------------------------------------------------------
 *
 * lock_graph.c
 *
 *  Functions for obtaining local and global lock graphs in which each
 *  node is a distributed transaction, and an edge represent a waiting-for
 *  relationship.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/hash.h"
#include "distributed/backend_data.h"
#include "distributed/connection_management.h"
#include "distributed/hash_helpers.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"


/*
 * Describes an edge in a waiting-for graph of locks.  This isn't used for
 * deadlock-checking directly, but to gather the information necessary to
 * do so.
 *
 * The datatypes here are a bit looser than strictly necessary, because
 * they're transported as the return type from an SQL function.
 */
typedef struct WaitEdge
{
	int waitingPid;
	int waitingNodeId;
	int64 waitingTransactionNum;
	TimestampTz waitingTransactionStamp;

	int blockingPid;
	int blockingNodeId;
	int64 blockingTransactionNum;
	TimestampTz blockingTransactionStamp;

	/* blocking transaction is also waiting on a lock */
	bool isBlockingXactWaiting;
} WaitEdge;


/*
 * WaitGraph represent a graph of wait edges as an adjacency list.
 */
typedef struct WaitGraph
{
	int localNodeId;
	size_t numAllocated;
	size_t numUsed;
	WaitEdge *edges;
} WaitGraph;


/*
 * PROCStack is a stack of PGPROC pointers used to perform a depth-first search
 * through the lock graph.
 */
typedef struct PROCStack
{
	int procCount;
	PGPROC **procs;
} PROCStack;


static void AddRemoteWaitEdges(WaitGraph *waitGraph);
static void AddWaitEdgeFromResult(WaitGraph *waitGraph, PGresult *result, int rowIndex);
static int64 ParseIntField(PGresult *result, int rowIndex, int colIndex);
static bool ParseBoolField(PGresult *result, int rowIndex, int colIndex);
static TimestampTz ParseTimestampTzField(PGresult *result, int rowIndex, int colIndex);
static void ReturnWaitGraph(WaitGraph *waitGraph, FunctionCallInfo fcinfo);
static WaitGraph * BuildWaitGraphForSourceNode(int sourceNodeId);
static void LockLockData(void);
static void UnlockLockData(void);
static void AddEdgesForLockWaits(WaitGraph *waitGraph, PGPROC *waitingProc,
								 PROCStack *remaining);
static void AddEdgesForWaitQueue(WaitGraph *waitGraph, PGPROC *waitingProc,
								 PROCStack *remaining);
static void AddWaitEdge(WaitGraph *waitGraph, BackendData *waitingXactData,
						PGPROC *waitingProc, BackendData *blockingXactData,
						PGPROC *blockingProc, bool isBlockingProcessWaiting);
static WaitEdge * AllocWaitEdge(WaitGraph *waitGraph);
static bool IsProcessWaitingForLock(PGPROC *proc);
static bool IsInDistributedTransaction(BackendData *backendData);
static bool IsSameLockGroup(PGPROC *leftProc, PGPROC *rightProc);


PG_FUNCTION_INFO_V1(dump_local_wait_edges);
PG_FUNCTION_INFO_V1(dump_global_wait_edges);


/*
 * dump_global_wait_edges returns global wait edges for distributed transactions
 * originating from the node on which it is started.
 */
Datum
dump_global_wait_edges(PG_FUNCTION_ARGS)
{
	int32 localNodeId = 0;
	WaitGraph *waitGraph = NULL;

	localNodeId = GetLocalGroupId();
	waitGraph = BuildWaitGraphForSourceNode(localNodeId);

	AddRemoteWaitEdges(waitGraph);

	ReturnWaitGraph(waitGraph, fcinfo);

	return (Datum) 0;
}


/*
 * AddRemoteWaitEdges adds all wait edges on (other) worker nodes to the given
 * graph.
 */
static void
AddRemoteWaitEdges(WaitGraph *waitGraph)
{
	List *workerNodeList = ActiveWorkerNodeList();
	ListCell *workerNodeCell = NULL;
	char *nodeUser = CitusExtensionOwnerName();
	List *connectionList = NIL;
	ListCell *connectionCell = NULL;

	/* open connections in parallel */
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		MultiConnection *connection = NULL;
		int connectionFlags = 0;

		if (workerNode->groupId == GetLocalGroupId())
		{
			/* don't include local wait edges, we can get these without network I/O */
			continue;
		}

		connection = StartNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort,
													 nodeUser, NULL);

		connectionList = lappend(connectionList, connection);
	}

	FinishConnectionListEstablishment(connectionList);

	/* send commands in parallel */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		int querySent = false;
		char *command = NULL;
		const char *params[1];

		params[0] = psprintf("%d", GetLocalGroupId());
		command = "SELECT * FROM dump_local_wait_edges($1)";

		querySent = SendRemoteCommandParams(connection, command, 1,
											NULL, params);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	/* receive dump_local_wait_edges results */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		PGresult *result = NULL;
		bool raiseInterrupts = true;
		int64 rowIndex = 0;
		int64 rowCount = 0;
		int64 colCount = 0;

		result = GetRemoteCommandResult(connection, raiseInterrupts);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, ERROR);
		}

		rowCount = PQntuples(result);
		colCount = PQnfields(result);

		if (colCount != 9)
		{
			ereport(ERROR, (errmsg("unexpected number of columns from "
								   "dump_local_wait_edges")));
		}

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			AddWaitEdgeFromResult(waitGraph, result, rowIndex);
		}

		PQclear(result);
		ForgetResults(connection);
	}
}


/*
 * AddWaitEdgeFromResult adds an edge to the wait graph that is read from
 * a PGresult.
 */
static void
AddWaitEdgeFromResult(WaitGraph *waitGraph, PGresult *result, int rowIndex)
{
	WaitEdge *waitEdge = AllocWaitEdge(waitGraph);

	waitEdge->waitingPid = ParseIntField(result, rowIndex, 0);
	waitEdge->waitingNodeId = ParseIntField(result, rowIndex, 1);
	waitEdge->waitingTransactionNum = ParseIntField(result, rowIndex, 2);
	waitEdge->waitingTransactionStamp = ParseTimestampTzField(result, rowIndex, 3);
	waitEdge->blockingPid = ParseIntField(result, rowIndex, 4);
	waitEdge->blockingNodeId = ParseIntField(result, rowIndex, 5);
	waitEdge->blockingTransactionNum = ParseIntField(result, rowIndex, 6);
	waitEdge->blockingTransactionStamp = ParseTimestampTzField(result, rowIndex, 7);
	waitEdge->isBlockingXactWaiting = ParseBoolField(result, rowIndex, 8);
}


/*
 * ParseIntField parses a int64 from a remote result or returns 0 if the
 * result is NULL.
 */
static int64
ParseIntField(PGresult *result, int rowIndex, int colIndex)
{
	char *resultString = NULL;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return 0;
	}

	resultString = PQgetvalue(result, rowIndex, colIndex);

	return pg_strtouint64(resultString, NULL, 10);
}


/*
 * ParseBoolField parses a bool from a remote result or returns false if the
 * result is NULL.
 */
static bool
ParseBoolField(PGresult *result, int rowIndex, int colIndex)
{
	char *resultString = NULL;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return false;
	}

	resultString = PQgetvalue(result, rowIndex, colIndex);

	return pg_strtouint64(resultString, NULL, 10);
}


/*
 * ParseTimestampTzField parses a timestamptz from a remote result or returns
 * 0 if the result is NULL.
 */
static TimestampTz
ParseTimestampTzField(PGresult *result, int rowIndex, int colIndex)
{
	char *resultString = NULL;
	Datum resultStringDatum = 0;
	Datum timestampDatum = 0;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return 0;
	}

	resultString = PQgetvalue(result, rowIndex, colIndex);
	resultStringDatum = CStringGetDatum(resultString);
	timestampDatum = DirectFunctionCall3(timestamptz_in, resultStringDatum, 0, -1);

	return DatumGetTimestampTz(timestampDatum);
}


/*
 * dump_local_wait_edges returns wait edges for distributed transactions
 * running on the node on which it is called, which originate from the source node.
 */
Datum
dump_local_wait_edges(PG_FUNCTION_ARGS)
{
	int32 sourceNodeId = PG_GETARG_INT32(0);

	WaitGraph *waitGraph = BuildWaitGraphForSourceNode(sourceNodeId);
	ReturnWaitGraph(waitGraph, fcinfo);

	return (Datum) 0;
}


/*
 * ReturnWaitGraph returns a wait graph for a set returning function.
 */
static void
ReturnWaitGraph(WaitGraph *waitGraph, FunctionCallInfo fcinfo)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc tupdesc = NULL;
	Tuplestorestate *tupstore = NULL;
	MemoryContext per_query_ctx = NULL;
	MemoryContext oldcontext = NULL;
	size_t curEdgeNum = 0;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "set-valued function called in context that cannot accept a set")));
	}
	if (!(rsinfo->allowedModes & SFRM_Materialize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));
	}

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
	{
		elog(ERROR, "return type must be a row type");
	}

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Columns:
	 * 00: waiting_pid
	 * 01: waiting_node_id
	 * 02: waiting_transaction_num
	 * 03: waiting_transaction_stamp
	 * 04: blocking_pid
	 * 05: blocking__node_id
	 * 06: blocking_transaction_num
	 * 07: blocking_transaction_stamp
	 * 08: blocking_transaction_waiting
	 */
	for (curEdgeNum = 0; curEdgeNum < waitGraph->numUsed; curEdgeNum++)
	{
		Datum values[9];
		bool nulls[9];
		WaitEdge *curEdge = &waitGraph->edges[curEdgeNum];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = Int32GetDatum(curEdge->waitingPid);
		values[1] = Int32GetDatum(curEdge->waitingNodeId);
		if (curEdge->waitingTransactionNum != 0)
		{
			values[2] = Int64GetDatum(curEdge->waitingTransactionNum);
			values[3] = TimestampTzGetDatum(curEdge->waitingTransactionStamp);
		}
		else
		{
			nulls[2] = true;
			nulls[3] = true;
		}

		values[4] = Int32GetDatum(curEdge->blockingPid);
		values[5] = Int32GetDatum(curEdge->blockingNodeId);
		if (curEdge->blockingTransactionNum != 0)
		{
			values[6] = Int64GetDatum(curEdge->blockingTransactionNum);
			values[7] = TimestampTzGetDatum(curEdge->blockingTransactionStamp);
		}
		else
		{
			nulls[6] = true;
			nulls[7] = true;
		}
		values[8] = BoolGetDatum(curEdge->isBlockingXactWaiting);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
}


/*
 * BuildWaitGraphForSourceNode builds a wait graph for distributed transactions
 * that originate from the given source node.
 */
static WaitGraph *
BuildWaitGraphForSourceNode(int sourceNodeId)
{
	WaitGraph *waitGraph = NULL;
	int curBackend = 0;
	bool visited[MaxBackends];
	PROCStack remaining;

	memset(visited, 0, MaxBackends);

	/*
	 * Try hard to avoid allocations while holding lock. Thus we pre-allocate
	 * space for locks in large batches - for common scenarios this should be
	 * more than enough space to build the list of wait edges without a single
	 * allocation.
	 */
	waitGraph = (WaitGraph *) palloc0(sizeof(WaitGraph));
	waitGraph->localNodeId = GetLocalGroupId();
	waitGraph->numAllocated = MaxBackends * 3;
	waitGraph->numUsed = 0;
	waitGraph->edges = (WaitEdge *) palloc(waitGraph->numAllocated * sizeof(WaitEdge));

	remaining.procs = (PGPROC **) palloc(sizeof(PGPROC *) * MaxBackends);
	remaining.procCount = 0;

	LockLockData();

	/*
	 * Build lock-graph.  We do so by first finding all procs which we are
	 * interested in (originating on our source system, and blocked).  Once
	 * those are collected, do depth first search over all procs blocking
	 * those.  To avoid redundantly dropping procs, keep track of which procs
	 * already have been visisted in a pgproc indexed visited[] array.
	 */

	/* build list of starting procs */
	for (curBackend = 0; curBackend < MaxBackends; curBackend++)
	{
		PGPROC *currentProc = &ProcGlobal->allProcs[curBackend];
		BackendData *currentBackendData = NULL;

		/* skip if the PGPROC slot is unused */
		if (currentProc->pid == 0)
		{
			continue;
		}

		currentBackendData = GetBackendDataForProc(currentProc);

		/*
		 * Only start searching from distributed transactions originating on the source
		 * node. Other deadlocks may exist, but the source node can only resolve those
		 * that involve its own transactions.
		 */
		if (sourceNodeId != currentBackendData->transactionId.initiatorNodeIdentifier ||
			!IsInDistributedTransaction(currentBackendData))
		{
			continue;
		}

		/* skip if the process is not blocked */
		if (!IsProcessWaitingForLock(currentProc))
		{
			continue;
		}

		remaining.procs[remaining.procCount++] = currentProc;
	}

	while (remaining.procCount > 0)
	{
		PGPROC *waitingProc = remaining.procs[--remaining.procCount];

		/*
		 * We might find a process again if multiple distributed transactions are
		 * waiting for it, but this won't result in any new edges. Moreover, if there
		 * is a local deadlock we might keep going forever. Therefore we skip processes
		 * that we've already visited.
		 */
		if (visited[waitingProc->pgprocno])
		{
			continue;
		}

		visited[waitingProc->pgprocno] = true;

		/* only blocked processes result in wait edges */
		if (!IsProcessWaitingForLock(waitingProc))
		{
			continue;
		}

		/*
		 * Record an edge for everyone already holding the lock in a
		 * conflicting manner ("hard edges" in postgres parlance).
		 */
		AddEdgesForLockWaits(waitGraph, waitingProc, &remaining);

		/*
		 * Record an edge for everyone in front of us in the wait-queue
		 * for the lock ("soft edges" in postgres parlance).
		 */
		AddEdgesForWaitQueue(waitGraph, waitingProc, &remaining);
	}

	UnlockLockData();

	return waitGraph;
}


/*
 * LockLockData takes locks the shared lock data structure, which prevents
 * concurrent lock acquisitions/releases.
 */
static void
LockLockData(void)
{
	int partitionNum = 0;

	LockDistributedTransactionSharedMemory(LW_SHARED);

	for (partitionNum = 0; partitionNum < NUM_LOCK_PARTITIONS; partitionNum++)
	{
		LWLockAcquire(LockHashPartitionLockByIndex(partitionNum), LW_SHARED);
	}
}


/*
 * UnlockLockData unlocks the locks on the shared lock data structure in reverse
 * order since LWLockRelease searches the given lock from the end of the
 * held_lwlocks array.
 */
static void
UnlockLockData(void)
{
	int partitionNum = 0;

	for (partitionNum = NUM_LOCK_PARTITIONS - 1; partitionNum >= 0; partitionNum--)
	{
		LWLockRelease(LockHashPartitionLockByIndex(partitionNum));
	}

	UnlockDistributedTransactionSharedMemory();
}


/*
 * AddEdgesForLockWaits adds an edge to the wait graph for every lock that
 * waitingProc is waiting for.
 */
static void
AddEdgesForLockWaits(WaitGraph *waitGraph, PGPROC *waitingProc, PROCStack *remaining)
{
	BackendData *waitingBackendData = GetBackendDataForProc(waitingProc);

	/* the lock for which this process is waiting */
	LOCK *waitLock = waitingProc->waitLock;

	/* determine the conflict mask for the lock level used by the process */
	LockMethod lockMethodTable = GetLocksMethodTable(waitLock);
	int numLockModes = lockMethodTable->numLockModes;
	int conflictMask = lockMethodTable->conflictTab[waitingProc->waitLockMode];

	/* iterate through the queue of processes holding the lock */
	SHM_QUEUE *procLocks = &(waitLock->procLocks);
	PROCLOCK *procLock = (PROCLOCK *) SHMQueueNext(procLocks, procLocks,
												   offsetof(PROCLOCK, lockLink));

	while (procLock != NULL)
	{
		PGPROC *blockingProc = procLock->tag.myProc;

		/* a proc never blocks itself or any other lock group member */
		if (!IsSameLockGroup(waitingProc, blockingProc))
		{
			BackendData *blockingBackendData = GetBackendDataForProc(blockingProc);

			int lockMode = 1;

			/* check conflicts with every locktype held */
			for (; lockMode <= numLockModes; lockMode++)
			{
				if ((procLock->holdMask & LOCKBIT_ON(lockMode)) &&
					(conflictMask & LOCKBIT_ON(lockMode)))
				{
					bool isBlockingXactWaiting = IsProcessWaitingForLock(blockingProc);
					if (isBlockingXactWaiting)
					{
						remaining->procs[remaining->procCount++] = blockingProc;
					}

					AddWaitEdge(waitGraph, waitingBackendData, waitingProc,
								blockingBackendData, blockingProc, isBlockingXactWaiting);
				}
			}
		}

		procLock = (PROCLOCK *) SHMQueueNext(procLocks, &procLock->lockLink,
											 offsetof(PROCLOCK, lockLink));
	}
}


/*
 * AddEdgesForWaitQueue adds an edge to the wait graph for processes in front
 * of waitingProc in the wait queue that are trying to acquire a conflicting
 * lock.
 */
static void
AddEdgesForWaitQueue(WaitGraph *waitGraph, PGPROC *waitingProc, PROCStack *remaining)
{
	BackendData *waitingBackendData = GetBackendDataForProc(waitingProc);

	/* the lock for which this process is waiting */
	LOCK *waitLock = waitingProc->waitLock;

	/* determine the conflict mask for the lock level used by the process */
	LockMethod lockMethodTable = GetLocksMethodTable(waitLock);
	int conflictMask = lockMethodTable->conflictTab[waitingProc->waitLockMode];

	/* iterate through the wait queue */
	PROC_QUEUE *waitQueue = &(waitLock->waitProcs);
	int queueSize = waitQueue->size;
	PGPROC *currentProc = (PGPROC *) waitQueue->links.next;

	while (queueSize-- > 0 && currentProc != waitingProc)
	{
		/* skip processes from the same lock group and ones that don't conflict */
		if (!IsSameLockGroup(waitingProc, currentProc) &&
			(LOCKBIT_ON(currentProc->waitLockMode) & conflictMask) != 0)
		{
			BackendData *currentBackendData = GetBackendDataForProc(currentProc);

			bool isBlockingXactWaiting = IsProcessWaitingForLock(currentProc);
			if (isBlockingXactWaiting)
			{
				remaining->procs[remaining->procCount++] = currentProc;
			}

			AddWaitEdge(waitGraph, waitingBackendData, waitingProc, currentBackendData,
						currentProc, isBlockingXactWaiting);
		}

		currentProc = (PGPROC *) currentProc->links.next;
	}
}


/*
 * AddWaitEdge adds a new wait edge to a wait graph. The nodes in the graph are
 * transactions and an edge indicates the "waiting" transactions is blocked on
 * a lock held by the "blocking" transaction.
 */
static void
AddWaitEdge(WaitGraph *waitGraph, BackendData *waitingXactData,
			PGPROC *waitingProc, BackendData *blockingXactData,
			PGPROC *blockingProc, bool isBlockingProcessWaiting)
{
	WaitEdge *curEdge = AllocWaitEdge(waitGraph);

	curEdge->waitingPid = waitingProc->pid;

	if (IsInDistributedTransaction(waitingXactData))
	{
		curEdge->waitingNodeId = waitingXactData->transactionId.initiatorNodeIdentifier;
		curEdge->waitingTransactionNum = waitingXactData->transactionId.transactionNumber;
		curEdge->waitingTransactionStamp = waitingXactData->transactionId.timestamp;
	}
	else
	{
		curEdge->waitingNodeId = waitGraph->localNodeId;
		curEdge->waitingTransactionNum = 0;
		curEdge->waitingTransactionStamp = 0;
	}

	curEdge->blockingPid = blockingProc->pid;

	if (IsInDistributedTransaction(blockingXactData))
	{
		curEdge->blockingNodeId = blockingXactData->transactionId.initiatorNodeIdentifier;
		curEdge->blockingTransactionNum =
			blockingXactData->transactionId.transactionNumber;
		curEdge->blockingTransactionStamp = blockingXactData->transactionId.timestamp;
	}
	else
	{
		curEdge->blockingNodeId = waitGraph->localNodeId;
		curEdge->blockingTransactionNum = 0;
		curEdge->blockingTransactionStamp = 0;
	}

	curEdge->isBlockingXactWaiting = isBlockingProcessWaiting;
}


/*
 * AllocWaitEdge allocates a wait edge as part of the given wait graph.
 * If the wait graph has insufficient space its size is doubled using
 * repalloc.
 */
static WaitEdge *
AllocWaitEdge(WaitGraph *waitGraph)
{
	/* ensure space for new edge */
	if (waitGraph->numAllocated == waitGraph->numUsed)
	{
		waitGraph->numAllocated *= 2;
		waitGraph->edges = (WaitEdge *)
						   repalloc(waitGraph->edges, sizeof(WaitEdge) *
									waitGraph->numAllocated);
	}

	return &waitGraph->edges[waitGraph->numUsed++];
}


/*
 * IsProcessWaitingForLock returns whether a given process is waiting for a lock.
 */
static bool
IsProcessWaitingForLock(PGPROC *proc)
{
	return proc->links.next != NULL && proc->waitLock != NULL;
}


/*
 * IsInDistributedTransaction returns whether the given back is in a distributed
 * transaction.
 */
static bool
IsInDistributedTransaction(BackendData *backendData)
{
	return backendData->transactionId.transactionNumber != 0;
}


/*
 * IsSameLockGroup returns whether two processes are part of the same lock group,
 * meaning they are either the same process, or have the same lock group leader.
 */
static bool
IsSameLockGroup(PGPROC *leftProc, PGPROC *rightProc)
{
	return leftProc == rightProc ||
		   (leftProc->lockGroupLeader != NULL &&
			leftProc->lockGroupLeader == rightProc->lockGroupLeader);
}
