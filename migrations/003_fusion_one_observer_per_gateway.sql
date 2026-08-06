-- One gateway cannot contribute two different local broadcast rounds to one
-- logical global broadcast.  Run after rebuilding derived fusion tables.
ALTER TABLE device_broadcast_observers
    ADD CONSTRAINT uq_observer_global_gateway UNIQUE (global_session_id, gateway_id);
