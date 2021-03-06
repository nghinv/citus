/*-------------------------------------------------------------------------
 *
 * maintenanced.h
 *	  Background worker run for each citus using database in a postgres
 *    cluster.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MAINTENANCED_H
#define MAINTENANCED_H

extern void InitializeMaintenanceDaemon(void);
extern void InitializeMaintenanceDaemonBackend(void);

extern void CitusMaintenanceDaemonMain(Datum main_arg);

#endif /* MAINTENANCED_H */
