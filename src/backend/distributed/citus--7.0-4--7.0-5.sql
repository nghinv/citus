/* citus--7.0-4--7.0-5.sql */

CREATE FUNCTION pg_catalog.dump_local_wait_edges(
                    IN source_node_id int,
                    OUT waiting_pid int4,
                    OUT waiting_node_id int4,
                    OUT waiting_transaction_num int8,
                    OUT waiting_transaction_stamp timestamptz,
                    OUT blocking_pid int,
                    OUT blocking_node_id int4,
                    OUT blocking_transaction_num int8,
                    OUT blocking_transaction_stamp timestamptz,
                    OUT blocking_transaction_waiting bool)
RETURNS SETOF RECORD
LANGUAGE 'c' STRICT
AS $$citus$$, $$dump_local_wait_edges$$;
COMMENT ON FUNCTION pg_catalog.dump_local_wait_edges(int)
IS 'returns a list of blocked transactions and the transaction they are waiting for';

CREATE FUNCTION pg_catalog.dump_global_wait_edges(
                    OUT waiting_pid int4,
                    OUT waiting_node_id int4,
                    OUT waiting_transaction_num int8,
                    OUT waiting_transaction_stamp timestamptz,
                    OUT blocking_pid int,
                    OUT blocking_node_id int4,
                    OUT blocking_transaction_num int8,
                    OUT blocking_transaction_stamp timestamptz,
                    OUT blocking_transaction_waiting bool)
RETURNS SETOF RECORD
LANGUAGE 'c' STRICT
AS $$citus$$, $$dump_global_wait_edges$$;
COMMENT ON FUNCTION pg_catalog.dump_global_wait_edges()
IS 'returns a list of blocked transactions and the transaction they are waiting for';
