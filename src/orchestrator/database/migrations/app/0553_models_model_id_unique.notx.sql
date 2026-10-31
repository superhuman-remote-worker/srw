-- migration:     0553_models_model_id_unique.notx.sql
-- description:   A model id names one catalog row across every provider.
--                Create and update map a violation to a 409 that names the
--                provider holding the id; the seeders skip the entry.
-- depends-on:    0552_models_served_model_id.sql
-- expected:      short; the catalog holds tens of rows
-- locks:         ShareUpdateExclusiveLock; writes remain available
-- transactional: NO

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_models_model_id ON public.models(model_id);
