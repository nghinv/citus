/*-------------------------------------------------------------------------
 *
 * multi_progress.c
 *	  Routines for tracking long-running jobs and seeing their progress.
 *
 * Copyright (c) 2012-2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


/*
 * CreateProgressMonitor is used to create some space to 
 */
void * CreateProgressMonitor(int progressTypeMagicNumber, int stepCount, Size stepSize)
{
  dsm_segment *dsmSegment = NULL;
  dsm_handle dsmHandle = 0;
  void *dsmSegmentAddress = NULL;
  ProgressMonitorHeader *header = NULL;
  void *taskInfoList = NULL;
  Size monitorSize = 0;

  monitorSize = sizeof(ProgressMonitorHeader) + stepSize * stepCount;
  dsmSegment = dsm_create(monitorSize, DSM_CREATE_NULL_IF_MAXSEGMENTS);

  if (dsmSegment == NULL)
  {
    /* TODO: Error */
  }

  dsmSegmentAddress = dsm_segment_address(dsmSegment);
  dsmHandle = dsm_segment_handle(dsmSegment);

  MonitorInfoFromAddress(dsmSegmentAddress, &header, &taskInfoList);

  header->stepCount = stepCount;

  pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM, relationId);
  pgstat_progress_update_param(1, dsmHandle);
  pgstat_progress_update_param(0, progressTypeMagicNumber);

  return taskInfoList;
}


/*
 * MonitorInfoFromSegmentAddress returns the header and event array parts from the given
 * address.
 */
static void
MonitorInfoFromAddress(void *segmentAddress, ProgressMonitorHeader **header,
							  void **rebalanceEvents)
{
	RebalanceMonitorHeader *headerAddress =
		(RebalanceMonitorHeader *) segmentAddress;
	*header = headerAddress;
	*rebalanceEvents = (void *) (headerAddress + 1);
}
