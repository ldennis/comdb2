/*
   Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#include "schemachange.h"
#include "schemachange_int.h"
#include "sc_logic.h"
#include "analyze.h"
#include "logmsg.h"

/**** Utility functions */

static enum thrtype prepare_sc_thread(struct schema_change_type *s)
{
    struct thr_handle *thr_self = thrman_self();
    enum thrtype oldtype = THRTYPE_UNKNOWN;

    if (!s->partialuprecs)
        logmsg(LOGMSG_DEBUG, "Starting a schemachange thread\n");

    if (!s->nothrevent) {
        if (thr_self) {
            thread_started("schema change");
            oldtype = thrman_get_type(thr_self);
            thrman_change_type(thr_self, THRTYPE_SCHEMACHANGE);
        } else
            thr_self = thrman_register(THRTYPE_SCHEMACHANGE);
        if (!s->nothrevent)
            backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);
    }
    return oldtype;
}

static void reset_sc_thread(enum thrtype oldtype, struct schema_change_type *s)
{
    struct thr_handle *thr_self = thrman_self();
    if (!s->nothrevent) {
        backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);

        /* restore our  thread type to what it was before */
        if (oldtype != THRTYPE_UNKNOWN)
            thrman_change_type(thr_self, oldtype);
    }
}

/* if we're using the low level meta table and we are doing a normal change,
 * mark the table as being in a schema change so that if we are interrupted
 * the new master knows to resume 
 * We mark schemachange over in mark_schemachange_over()
 */
static int mark_sc_in_llmeta(struct schema_change_type *s)
{
    int bdberr;
    int rc = SC_OK;
    void *packed_sc_data = NULL;
    size_t packed_sc_data_len;
    if (pack_schema_change_type(s, &packed_sc_data, &packed_sc_data_len)) {
        sc_errf(s, "could not pack the schema change data for storage in "
                   "low level meta table\n");
        return SC_LLMETA_ERR;
    } else {
        unsigned retries;
        unsigned max_retries = 10;

        /* mark the schema change in progress in the low level meta table,
         * retry several times */
        for (retries = 0;
             retries < max_retries &&
                 (bdb_set_in_schema_change(NULL /*input_trans*/, s->table,
                                           packed_sc_data, packed_sc_data_len,
                                           &bdberr) ||
                  bdberr != BDBERR_NOERROR);
             ++retries) {
            sc_errf(s, "could not mark schema change in progress in the "
                       "low level meta table, retrying ...\n");
            sleep(1);
        }
        if (retries >= max_retries) {
            sc_errf(s, "could not mark schema change in progress in the "
                       "low level meta table, giving up after %u retries\n",
                    retries);
            rc = SC_LLMETA_ERR;
            if (s->resume) {
                sc_errf(s, "failed to resume schema change, downgrading to "
                           "give another master a shot\n");
                bdb_transfermaster(thedb->dbs[0]->handle);
            }
        }
    }

    free(packed_sc_data);
    return rc;
}


static int propose_sc(struct schema_change_type *s)
{
    /* Check that all nodes are ready to do this schema change. */
    int rc = broadcast_sc_start(sc_seed, gbl_mynode, time(NULL));
    if (rc != 0) {
        rc = SC_PROPOSE_FAIL;
        sc_errf(s, "unable to gain agreement from all nodes to do schema "
                   "change\n");
        sc_errf(s, "check that all nodes are connected ('send bdb "
                   "cluster')\n");
    } else {
        /* if we're not actually changing the schema then everything is fully
         * replicated so we don't actually need all the replicants online to
         * do this safely.  this helps save fastinit. */
        if (!s->same_schema) {
            if (check_sc_ok(s) != 0) {
                rc = SC_PROPOSE_FAIL;
            } else {
                rc = broadcast_sc_ok();
                if (rc != 0) {
                    sc_errf(s, "cannot perform schema change; not all nodes "
                               "acknowledged readiness\n");
                    rc = SC_PROPOSE_FAIL;
                }
            }
        }

        if (s->force) {
            sc_printf(s, "Performing schema change regardless in force "
                         "mode\n");
            rc = SC_OK;
        }
    }
    return rc;
}

static int master_downgrading(struct schema_change_type *s)
{
    if (stopsc) {
        if (!s->nothrevent)
            backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
        if(s->sb) {
            sbuf2printf(
                s->sb,
                "!Master node downgrading - new master will resume schemachange\n");
            sbuf2close(s->sb);
        }
        logmsg(LOGMSG_WARN, "Master node downgrading - new master will resume schemachange\n");
        gbl_schema_change_in_progress = 0;
        stopsc = 0;
        return SC_MASTER_DOWNGRADE;
    }
    return SC_OK;
}

static void stop_and_free_sc(int rc, struct schema_change_type *s)
{
    if (!s->partialuprecs) {
        if (rc != 0) {
            logmsg(LOGMSG_INFO, "Schema change returning FAILED\n");
            sbuf2printf(s->sb, "FAILED\n");
        } else {
            logmsg(LOGMSG_INFO, "Schema change returning SUCCESS\n");
            sbuf2printf(s->sb, "SUCCESS\n");
        }
    }
    sc_set_running(0, sc_seed, NULL, 0);

    free_schema_change_type(s);
    /* free any memory csc2 allocated when parsing schema */
    csc2_free_all();
}

static int set_original_tablename(struct schema_change_type *s)
{
    struct db *db = getdbbyname(s->table);
    if (db) {
        strncpy0(s->table, db->dbname, sizeof(s->table));
        return 0;
    }
    return 1;
}

/*********** Outer Business logic for schemachanges ************************/

char *generate_history_csc2(struct db *db)
{
    struct schema *schema;
    int field;
    strbuf *csc2;
    int ixnum;
    char buf[128];
    char *outcsc2 = NULL;

    schema = db->schema;
    csc2 = strbuf_new();
    strbuf_clear(csc2);

    strbuf_append(csc2, "\n");
    strbuf_append(csc2, "tag ondisk\n");
    strbuf_append(csc2, "{\n");
    for (field = 0; field < schema->nmembers; field++) {
        strbuf_append(csc2, "\t");
        strbuf_append(csc2, csc2type(&schema->member[field]));
        strbuf_append(csc2, "\t");
        strbuf_append(csc2, schema->member[field].name);
        switch (schema->member[field].type) {
        case SERVER_BYTEARRAY:
            strbuf_append(csc2, "[");
            snprintf(buf, 128, "%d", schema->member[field].len - 1);
            strbuf_append(csc2, buf);
            strbuf_append(csc2, "]");
            break;
        case SERVER_BCSTR:
        case CLIENT_CSTR:
        case CLIENT_PSTR2:
        case CLIENT_PSTR:
        case CLIENT_BYTEARRAY:
            strbuf_append(csc2, "[");
            snprintf(buf, 128, "%d", schema->member[field].len);
            strbuf_append(csc2, buf);
            strbuf_append(csc2, "]");
            break;
        case CLIENT_VUTF8:
        case SERVER_VUTF8:
            if (schema->member[field].len > 5) {
                strbuf_append(csc2, "[");
                snprintf(buf, 128, "%d", schema->member[field].len - 5);
                strbuf_append(csc2, buf);
                strbuf_append(csc2, "]");
            }
            break;
        default:
            break;
        }
        if (!(schema->member[field].flags & NO_NULL))
            strbuf_append(csc2, " null=yes");
        if (schema->member[field].in_default) {
            strbuf_append(csc2, " dbstore=");
            strbuf_append(csc2,
                          sql_field_default_trans(&(schema->member[field]), 0));
        }
        if (schema->member[field].out_default) {
            strbuf_append(csc2, " dbload=");
            strbuf_append(csc2,
                          sql_field_default_trans(&(schema->member[field]), 1));
        }
        strbuf_append(csc2, "\n");
    }
    strbuf_append(csc2, "}\n");
    if (db->nix > 0) {
        strbuf_append(csc2, "keys\n");
        strbuf_append(csc2, "{\n");
        /* do the indices */
        for (ixnum = 0; ixnum < db->nix; ixnum++) {
            schema = db->ixschema[ixnum];
            strbuf_append(csc2, "\t dup ");
            if (schema->flags & SCHEMA_DATACOPY) {
                strbuf_append(csc2, "datacopy ");
            }
            if (schema->flags & SCHEMA_RECNUM) {
                strbuf_append(csc2, "recnum ");
            }
            strbuf_append(csc2, "\"");
            if (strncasecmp(schema->csctag, ".NEW.", 5) == 0)
                strbuf_append(csc2, schema->csctag + 5);
            else
                strbuf_append(csc2, schema->csctag);
            strbuf_append(csc2, "\" = ");
            for (field = 0; field < schema->nmembers; field++) {
                if (field > 0)
                    strbuf_append(csc2, " + ");
                if (schema->member[field].flags & INDEX_DESCEND)
                    strbuf_append(csc2, "<DESCEND> ");
                if (schema->member[field].isExpr) {
                    strbuf_append(csc2, "(");
                    strbuf_append(csc2, csc2type(&schema->member[field]));
                    switch (schema->member[field].type) {
                    case SERVER_BYTEARRAY:
                        strbuf_append(csc2, "[");
                        snprintf(buf, 128, "%d", schema->member[field].len - 1);
                        strbuf_append(csc2, buf);
                        strbuf_append(csc2, "]");
                        break;
                    case SERVER_BCSTR:
                    case CLIENT_CSTR:
                    case CLIENT_PSTR2:
                    case CLIENT_PSTR:
                    case CLIENT_BYTEARRAY:
                        strbuf_append(csc2, "[");
                        snprintf(buf, 128, "%d", schema->member[field].len);
                        strbuf_append(csc2, buf);
                        strbuf_append(csc2, "]");
                        break;
                    case CLIENT_VUTF8:
                    case SERVER_VUTF8:
                        if (schema->member[field].len > 5) {
                            strbuf_append(csc2, "[");
                            snprintf(buf, 128, "%d",
                                     schema->member[field].len - 5);
                            strbuf_append(csc2, buf);
                            strbuf_append(csc2, "]");
                        }
                        break;
                    default:
                        break;
                    }
                    strbuf_append(csc2, ")\"");
                }
                strbuf_append(csc2, schema->member[field].name);
                if (schema->member[field].isExpr) {
                    strbuf_append(csc2, "\"");
                }
            }
            if (schema->where) {
                strbuf_append(csc2, " {");
                strbuf_append(csc2, schema->where);
                strbuf_append(csc2, "}");
            }
            strbuf_append(csc2, "\n");
        }
        strbuf_append(csc2, "}\n");
    }
    outcsc2 = strdup(strbuf_buf(csc2));
    strbuf_free(csc2);
    return outcsc2;
}

int do_alter_table_shard(struct schema_change_type *s, struct ireq *iq,
                         int indx, int maxindx) 
{
    int rc;

    if(!s->timepart_dbs) {
        s->timepart_dbs = (struct db**)calloc(maxindx, sizeof(struct db*));
        s->timepart_newdbs = (struct db**)calloc(maxindx, sizeof(struct db*));
        s->timepart_nshards = maxindx;
    }


    set_original_tablename(s);

    if ((rc = mark_sc_in_llmeta(s)))
        return rc;

    propose_sc(s);

    rc = do_alter_table_int(s, iq);

    if(!rc) {
        s->timepart_dbs[indx] = s->db;
        s->timepart_newdbs[indx] = s->newdb;
    }

    mark_schemachange_over(NULL, s->table);

    if (rc)
    {
        /* clean previous indx schema changes */
    }

    return rc;
}

static void check_for_idx_rename(struct db *newdb, struct db *olddb)
{
    if (!newdb || !newdb->plan)
        return;

    for (int ixnum = 0; ixnum < newdb->nix; ixnum++) {
        struct schema *newixs = newdb->ixschema[ixnum];

        int oldixnum = newdb->plan->ix_plan[ixnum];
        if (oldixnum < 0 || oldixnum >= olddb->nix)
            continue;

        struct schema *oldixs = olddb->ixschema[oldixnum];
        if (!oldixs)
            continue;

        int offset = get_offset_of_keyname(newixs->csctag);
        if (get_offset_of_keyname(oldixs->csctag) > 0) {
            logmsg(LOGMSG_USER, "WARN: Oldix has .NEW. in idx name: %s\n", oldixs->csctag);
            return;
        }
        if (newdb->plan->ix_plan[ixnum] >= 0 &&
            strcmp(newixs->csctag + offset, oldixs->csctag) != 0) {
            char namebuf1[128];
            char namebuf2[128];
            form_new_style_name(namebuf1, sizeof(namebuf1), newixs,
                                newixs->csctag + offset, newdb->dbname);
            form_new_style_name(namebuf2, sizeof(namebuf2), oldixs,
                                oldixs->csctag, olddb->dbname);
            logmsg(LOGMSG_USER, 
                        "ix %d changing name so INSERTING into sqlite_stat* "
                        "idx='%s' where tbl='%s' and idx='%s' \n",
                ixnum, newixs->csctag + offset, newdb->dbname, oldixs->csctag);
            add_idx_stats(newdb->dbname, namebuf2, namebuf1);
        }
    }
}

static int do_alter_table_common(struct schema_change_type *s, struct ireq *iq)
{
    int rc;
    set_original_tablename(s);
    int pre_is_temproal, post_is_temproal;
    int defer_finalize = 0;
    pre_is_temproal = post_is_temproal = 0;

    if ((rc = mark_sc_in_llmeta(s)))
        return rc;

    if (rc == SC_OK)
        rc = do_alter_table_int(s, iq);

    if (master_downgrading(s))
        return SC_MASTER_DOWNGRADE;

    if (s->is_history == 0 && s->db && s->db->periods[PERIOD_SYSTEM].enable)
        pre_is_temproal = 1;
    if (s->is_history == 0 && s->newdb &&
        s->newdb->periods[PERIOD_SYSTEM].enable)
        post_is_temproal = 1;
    if (pre_is_temproal || post_is_temproal) {
        /* wait for history table */
        defer_finalize = 1;
        if (!pre_is_temproal && post_is_temproal)
            s->add_history = 1;
        else if (pre_is_temproal && !post_is_temproal)
            s->drop_history = 1;
        else
            s->alter_history = 1;
    }

    if (rc) {
        mark_schemachange_over(NULL, s->table);
    } else if (s->finalize && !defer_finalize) {
        if (s->type == DBTYPE_TAGGED_TABLE && !s->timepart_nshards) {
            /* check for rename outside of taking schema lock */
            /* handle renaming sqlite_stat1 entries for idx */
            check_for_idx_rename(s->newdb, s->db);
        }
        wrlock_schema_lk();
        rc = finalize_alter_table(s);
        unlock_schema_lk();
    } else {
        rc = SC_COMMIT_PENDING;
    }
    return rc;
}

static int do_alter_history(struct schema_change_type *s, struct db *db,
                            struct ireq *iq);
static int do_drop_history(struct schema_change_type *s, struct db *db,
                           struct ireq *iq);
static int do_add_history(struct schema_change_type *s, struct db *db,
                          struct ireq *iq);
/* Schema change thread.  We must already have set the schema change running
 * flag and the seed in sc_seed. */
static int do_alter_table(struct schema_change_type *s, struct ireq *iq)
{
#ifdef DEBUG
    printf("do_alter_table() %s\n", s->resume?"resuming":"");
#endif
    int rc, outrc;

    if (!s->resume)
        set_sc_flgs(s);

    if (!timepart_is_timepart(s->table, 1) &&
        /* resuming a stopped view sc */
        !(s->resume && timepart_is_shard(s->table, 1))) {
        outrc = rc = do_alter_table_common(s, iq);
        if (rc != SC_OK && rc != SC_COMMIT_PENDING)
            goto end;
        if (s->add_history || s->drop_history || s->alter_history) {
            if (s->add_history) {
                /* add history table */
                wrlock_schema_lk();
                rc = do_add_history(s, s->newdb, iq);
                unlock_schema_lk();
                if (rc != SC_OK) {
                    outrc = rc;
                    goto end;
                }
            } else if (s->drop_history) {
                /* drop history table */
                wrlock_schema_lk();
                rc = do_drop_history(s, s->db, iq);
                unlock_schema_lk();
                if (rc != SC_OK) {
                    outrc = rc;
                    goto end;
                }
            } else {
                /* alter both current and history tables */
                rc = do_alter_history(s, s->newdb, iq);
                if (rc != SC_OK) {
                    outrc = rc;
                    goto end;
                }
            }
            if (s->finalize) {
                if (s->type == DBTYPE_TAGGED_TABLE && !s->timepart_nshards) {
                    /* check for rename outside of taking schema lock */
                    /* handle renaming sqlite_stat1 entries for idx */
                    check_for_idx_rename(s->newdb, s->db);
                }
                wrlock_schema_lk();
                outrc = finalize_alter_table(s);
                unlock_schema_lk();
            }
        }
    } else {
        outrc = timepart_alter_timepart(s, iq, s->table, do_alter_table_shard);
    }

end:
    broadcast_sc_end(sc_seed);
    return outrc;
}

static inline int init_history_sc(struct schema_change_type *s, struct db *db,
                                  struct schema_change_type *scopy)
{
    scopy->sb = s->sb;
    scopy->must_close_sb = 0;
    scopy->nothrevent = 1;
    scopy->live = 1;
    scopy->type = DBTYPE_TAGGED_TABLE;
    scopy->finalize = 0;
    if (strlen(db->dbname) + strlen("_history") + 1 > MAXTABLELEN) {
        sc_errf(s, "History table name too long\n");
        free_schema_change_type(scopy);
        return SC_CSC2_ERROR;
    }
    snprintf(scopy->table, sizeof(scopy->table), "%s_history", db->dbname);
    scopy->table[sizeof(scopy->table) - 1] = '\0';
    if (scopy->newcsc2)
        free(scopy->newcsc2);
    scopy->newcsc2 = NULL;
    scopy->headers = s->headers;
    scopy->compress = s->compress;
    scopy->compress_blobs = s->compress_blobs;
    scopy->ip_updates = s->ip_updates;
    scopy->instant_sc = s->instant_sc;
    scopy->force_rebuild = s->force_rebuild;
    scopy->force_dta_rebuild = s->force_dta_rebuild;
    scopy->force_blob_rebuild = s->force_blob_rebuild;
    scopy->is_history = 1;
    scopy->orig_db = db;
    s->history_s = scopy;

    return 0;
}

static int do_alter_history(struct schema_change_type *s, struct db *db,
                            struct ireq *iq)
{
    struct schema_change_type *scopy = new_schemachange_type();
    if (init_history_sc(s, db, scopy)) {
        if (iq)
            reqerrstr(iq, ERR_SC, "History table name too long");
        return SC_CSC2_ERROR;
    }
    scopy->alteronly = 1;
    scopy->use_plan = 1;
    scopy->scanmode = SCAN_PARALLEL;
    scopy->newcsc2 = generate_history_csc2(db);

    s->history_rc = do_alter_table_common(scopy, iq);
    if (s->history_rc != SC_OK && s->history_rc != SC_COMMIT_PENDING) {
        free_schema_change_type(scopy);
        s->history_s = NULL;
        if (iq)
            reqerrstr(iq, ERR_SC, "Failed to alter history table");
        sc_errf(s, "error altering history table\n");
        logmsg(LOGMSG_ERROR, "%s failed with rc %d\n", __func__, s->history_rc);
        return s->history_rc;
    }

    scopy->newdb->is_history_table = 1;
    s->newdb->history_db = scopy->newdb;
    scopy->newdb->orig_db = s->newdb;

    sc_printf(s, "Alter history table %s ok\n", scopy->table);
    return SC_OK;
}

int do_upgrade_table(struct schema_change_type *s)
{
    int rc;

    set_original_tablename(s);

    if (!s->resume)
        set_sc_flgs(s);
    if ((rc = mark_sc_in_llmeta(s)))
        return rc;

    if (rc == SC_OK)
        rc = do_upgrade_table_int(s);

    if (rc) {
        mark_schemachange_over(NULL, s->table);
    } else if (s->finalize) {
        rc = finalize_upgrade_table(s);
    } else {
        rc = SC_COMMIT_PENDING;
    }

    return rc;
}

static inline int do_fastinit_common(struct schema_change_type *s,
                                     struct ireq *iq)
{
#ifdef DEBUG
    printf("do_fastinit() %s\n", s->resume?"resuming":"");
#endif
    int rc;
    int defer_finalize = 0;
    set_original_tablename(s);

    if (!s->resume)
        set_sc_flgs(s);

    if ((rc = mark_sc_in_llmeta(s)))
        return rc;

    propose_sc(s);
    rc = do_fastinit_int(s, iq);

    /* wait for history table */
    if (s->is_history == 0 && s->db && s->db->periods[PERIOD_SYSTEM].enable) {
        defer_finalize = 1;
        s->drop_history = 1;
    }

    if (rc) {
        mark_schemachange_over(NULL, s->table);
    } else if (s->finalize && !defer_finalize) {
        rc = finalize_fastinit_table(s);
    } else {
        rc = SC_COMMIT_PENDING;
    }
    return rc;
}

static int do_drop_history(struct schema_change_type *s, struct db *db,
                           struct ireq *iq)
{
    struct schema_change_type *scopy = new_schemachange_type();
    if (init_history_sc(s, db, scopy)) {
        if (iq)
            reqerrstr(iq, ERR_SC, "History table name too long");
        return SC_CSC2_ERROR;
    }
    scopy->same_schema = 1;
    scopy->drop_table = 1;
    scopy->fastinit = 1;
    if (get_csc2_file(scopy->table, -1, &scopy->newcsc2, NULL)) {
        if (iq)
            reqerrstr(iq, ERR_SC, "History table %s schema not found",
                      scopy->table);
        sc_errf(s, "History table %s schema not found\n", scopy->table);
        free_schema_change_type(scopy);
        return SC_CSC2_ERROR;
    }

    s->history_rc = do_fastinit_common(scopy, iq);
    if (s->history_rc != SC_OK && s->history_rc != SC_COMMIT_PENDING) {
        free_schema_change_type(scopy);
        s->history_s = NULL;
        if (iq)
            reqerrstr(iq, ERR_SC, "Failed to delete history table");
        sc_errf(s, "error deleting history table\n");
        logmsg(LOGMSG_ERROR, "%s failed with rc %d\n", __func__, s->history_rc);
        return s->history_rc;
    }

    sc_printf(s, "Drop history table %s ok\n", scopy->table);
    return SC_OK;
}

int do_fastinit(struct schema_change_type *s, struct ireq *iq)
{
    int rc, outrc;

    wrlock_schema_lk();

    outrc = rc = do_fastinit_common(s, iq);
    if (rc != SC_OK && rc != SC_COMMIT_PENDING)
        goto end;

    if (s->drop_history) {
        if ((rc = do_drop_history(s, s->db, iq)) != SC_OK) {
            outrc = rc;
            goto end;
        }
        if (s->finalize) {
            outrc = finalize_fastinit_table(s);
            free_schema_change_type(s->history_s);
            s->history_s = NULL;
        }
    }

end:
    unlock_schema_lk();
    broadcast_sc_end(sc_seed);

    return outrc;
}

static inline int do_add_table_common(struct schema_change_type *s,
                                      struct ireq *iq)
{
    int rc;
    int defer_finalize = 0;
    set_original_tablename(s);

    if (!s->resume)
        set_sc_flgs(s);
    if ((rc = mark_sc_in_llmeta(s))) {
        return rc;
    }

    if (rc == SC_OK)
        rc = do_add_table_int(s, iq);

    /* wait for history table */
    if (s->is_history == 0 && s->db && s->db->periods[PERIOD_SYSTEM].enable) {
        defer_finalize = 1;
        s->add_history = 1;
    }

    if (rc) {
        mark_schemachange_over(NULL, s->table);
    } else if (s->finalize && !defer_finalize) {
        rc = finalize_add_table(s);
    } else {
        rc = SC_COMMIT_PENDING;
    }
    return rc;
}

static int do_add_history(struct schema_change_type *s, struct db *db,
                          struct ireq *iq)
{
    struct schema_change_type *scopy = new_schemachange_type();
    if (init_history_sc(s, db, scopy)) {
        if (iq)
            reqerrstr(iq, ERR_SC, "History table name too long");
        return SC_CSC2_ERROR;
    }
    scopy->addonly = 1;
    scopy->newcsc2 = generate_history_csc2(db);

    s->history_rc = do_add_table_common(scopy, iq);
    if (s->history_rc != SC_OK && s->history_rc != SC_COMMIT_PENDING) {
        free_schema_change_type(scopy);
        s->history_s = NULL;
        if (iq)
            reqerrstr(iq, ERR_SC, "Failed to add history table");
        sc_errf(s, "error adding history table\n");
        logmsg(LOGMSG_ERROR, "%s failed with rc %d\n", __func__, s->history_rc);
        return s->history_rc;
    }

    scopy->db->is_history_table = 1;
    s->db->history_db = scopy->db;
    scopy->db->orig_db = s->db;

    sc_printf(s, "Add history table %s ok\n", scopy->table);
    return SC_OK;
}

int do_add_table(struct schema_change_type *s, struct ireq *iq)
{
    int rc, outrc;

    wrlock_schema_lk();

    outrc = rc = do_add_table_common(s, iq);
    if (rc != SC_OK && rc != SC_COMMIT_PENDING) {
        goto end;
    }

    if (s->add_history) {
        if ((rc = do_add_history(s, s->db, iq)) != SC_OK) {
            outrc = rc;
            goto end;
        }
        /* finalize current table */
        if (s->finalize) {
            outrc = finalize_add_table(s);
            free_schema_change_type(s->history_s);
            s->history_s = NULL;
        }
    }

end:
    unlock_schema_lk();

    return outrc;
}

int do_alter_queues(struct schema_change_type *s)
{
    struct db *db;
    int rc, bdberr;

    set_original_tablename(s);

    if (!s->resume)
        set_sc_flgs(s);

    rc = propose_sc(s);

    if (rc == SC_OK)
        rc = do_alter_queues_int(s);

    if (master_downgrading(s))
        return SC_MASTER_DOWNGRADE;

    broadcast_sc_end(sc_seed);

    if ((s->type != DBTYPE_TAGGED_TABLE) && gbl_pushlogs_after_sc)
        push_next_log();

    return rc;
}

int do_alter_stripes(struct schema_change_type *s)
{
    struct db *db;
    int rc, bdberr;

    set_original_tablename(s);

    if (!s->resume)
        set_sc_flgs(s);

    rc = propose_sc(s);

    if (rc == SC_OK)
        rc = do_alter_stripes_int(s);

    if (master_downgrading(s))
        return SC_MASTER_DOWNGRADE;

    broadcast_sc_end(sc_seed);

    /* if we did a regular schema change and we used the llmeta we don't need to
     * push locgs */
    if ((s->type != DBTYPE_TAGGED_TABLE) && gbl_pushlogs_after_sc)
        push_next_log();

    return rc;
}

int do_schema_change_thd(struct sc_arg *arg)
{
    struct schema_change_type *s = arg->s;
    struct ireq *iq = arg->iq;
    free(arg);

    enum thrtype oldtype = prepare_sc_thread(s);
    int rc = SC_OK;

    if (s->addsp)
        rc = do_add_sp(s, iq);
    else if (s->delsp)
        rc = do_del_sp(s, iq);
    else if (s->defaultsp)
        rc = do_default_sp(s, iq);
    else if (s->showsp)
        rc = do_show_sp(s);
    else if (s->is_trigger)
        rc = perform_trigger_update(s);
    else if (s->is_sfunc)
        rc = do_lua_sfunc(s);
    else if (s->is_afunc)
        rc = do_lua_afunc(s);
    else if (s->fastinit)
        rc = do_fastinit(s, iq);
    else if (s->addonly)
        rc = do_add_table(s, iq);
    else if (s->fulluprecs || s->partialuprecs)
        rc = do_upgrade_table(s);
    else if (s->type == DBTYPE_TAGGED_TABLE)
        rc = do_alter_table(s, iq);
    else if (s->type == DBTYPE_QUEUE)
        rc = do_alter_queues(s);
    else if (s->type == DBTYPE_MORESTRIPE)
        rc = do_alter_stripes(s);

    if (s->history_s) {
        reset_sc_thread(oldtype, s->history_s);
        if (s->history_rc != SC_COMMIT_PENDING) {
            rc = s->history_rc;
            stop_and_free_sc(s->history_rc, s->history_s);
            s->history_s = NULL;
        }
    }
    reset_sc_thread(oldtype, s);
    if (rc != SC_COMMIT_PENDING && rc != SC_MASTER_DOWNGRADE)
        stop_and_free_sc(rc, s);

    return rc;
}

int finalize_schema_change_thd(struct schema_change_type *s)
{
    enum thrtype oldtype = prepare_sc_thread(s);
    int rc = SC_OK;

    if (s->type == DBTYPE_TAGGED_TABLE && !s->timepart_nshards) {
        /* check for rename outside of taking schema lock */
        /* handle renaming sqlite_stat1 entries for idx */
        check_for_idx_rename(s->newdb, s->db);
    }

    wrlock_schema_lk();
    if (s->is_trigger)
        rc = finalize_trigger(s);
    else if (s->is_sfunc)
        rc = finalize_lua_sfunc();
    else if (s->is_afunc)
        rc = finalize_lua_afunc();
    else if (s->fastinit)
        rc = finalize_fastinit_table(s);
    else if (s->addonly)
        rc = finalize_add_table(s);
    else if (s->type == DBTYPE_TAGGED_TABLE)
        rc = finalize_alter_table(s);
    else if (s->fulluprecs || s->partialuprecs)
        rc = finalize_upgrade_table(s);
    unlock_schema_lk();

    if (s->history_s) {
        free_schema_change_type(s->history_s);
        s->history_s = NULL;
    }
    reset_sc_thread(oldtype, s);
    stop_and_free_sc(rc, s);
    return rc;
}

int resume_schema_change(void)
{
    int i;
    int rc;
    int scabort = 0;

    /* if we're not the master node then we can't do schema change! */
    if (thedb->master != gbl_mynode) {
        logmsg(LOGMSG_WARN, "resume_schema_change: not the master, cannot resume a"
                        " schema change\n");
        return -1;
    }

    /* if a schema change is currently running don't try to resume one */
    pthread_mutex_lock(&schema_change_in_progress_mutex);
    if (gbl_schema_change_in_progress) {
        // we are just starting up or just became master
        gbl_schema_change_in_progress = 0; 
    }
    pthread_mutex_unlock(&schema_change_in_progress_mutex);

    for (i = 0; i < thedb->num_dbs; ++i) {
        int bdberr;
        void *packed_sc_data = NULL;
        size_t packed_sc_data_len;
        if (bdb_get_in_schema_change(thedb->dbs[i]->dbname, &packed_sc_data,
                                     &packed_sc_data_len, &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_WARN, "resume_schema_change: failed to discover "
                    "whether table: %s is in the middle of a schema change\n",
                    thedb->dbs[i]->dbname);
            continue;
        }

        /* if we got some data back, that means we were in a schema change */
        if (packed_sc_data) {
            struct schema_change_type *s;
            logmsg(LOGMSG_WARN, "resume_schema_change: table: %s is in the middle of a "
                   "schema change, resuming...\n",
                   thedb->dbs[i]->dbname);

            s = malloc(sizeof(struct schema_change_type));
            if (!s) {
                logmsg(LOGMSG_ERROR, "resume_schema_change: ran out of memory\n");
                free(packed_sc_data);
                return -1;
            }

            if (unpack_schema_change_type(s, packed_sc_data,
                                          packed_sc_data_len)) {
                sc_errf(s, "could not unpack the schema change data retrieved "
                           "from the low level meta table\n");
                free(packed_sc_data);
                free(s);
                return -1;
            }

            free(packed_sc_data);

            /* Give operators a chance to prevent a schema change from resuming.
             */
            char * abort_filename = comdb2_location("marker", "%s.scabort", 
                                                    thedb->envname);
            if (access(abort_filename, F_OK) == 0) {
                rc = bdb_set_in_schema_change(NULL, thedb->dbs[i]->dbname, NULL,
                                              0, &bdberr);
                if (rc)
                    logmsg(LOGMSG_ERROR, "Failed to cancel resuming schema change %d %d\n",
                            rc, bdberr);
                else
                    scabort = 1;
            }


            if (scabort) {
                logmsg(LOGMSG_WARN, "Cancelling schema change\n");
                rc = unlink(abort_filename);
                if (rc)
                    logmsg(LOGMSG_ERROR, "Can't delete abort marker file %s - "
                                    "future sc may abort\n",
                            abort_filename);
                free(abort_filename);
                free(s);
                return 0;
            }
            free(abort_filename);

            if (s->fulluprecs || s->partialuprecs) {
                logmsg(LOGMSG_DEBUG, "%s: This was a table upgrade. Skipping...\n", __func__);
                free(s);
                return 0;
            }
            if(s->type != DBTYPE_TAGGED_TABLE) { /* see do_schema_change_thd()*/
                logmsg(LOGMSG_ERROR, "%s: only type DBTYPE_TAGGED_TABLE can resume\n", __func__);
                free(s);
                return 0;
            }

            s->nothrevent = 0;
            s->resume = 1; /* we are trying to resume this sc */
            s->finalize = 1; /* finalize at the end of resume */

            MEMORY_SYNC;

            /* start the schema change back up */
            rc = start_schema_change(thedb, s, NULL);
            if (rc != SC_OK && rc != SC_ASYNC) {
                return -1;
            }

            return 0;
        }
    }

    return 0;
}

/****************** Table functions ***********************************/
/****************** Functions down here will likely be moved elsewhere *****/

/* this assumes threads are not active in db */
int open_temp_db_resume(struct db *db, char *prefix, int resume, int temp)
{
    char *tmpname;
    int bdberr;
    int nbytes;

    nbytes = snprintf(NULL, 0, "%s%s", prefix, db->dbname);
    if (nbytes <= 0)
        nbytes = 2;
    nbytes++;
    if (nbytes > 32)
        nbytes = 32;
    tmpname = malloc(nbytes);
    snprintf(tmpname, nbytes, "%s%s", prefix, db->dbname);

    db->handle = NULL;

    /* open existing temp db if it's there (ie we're resuming after a master
     * switch) */
    if (resume) {
        db->handle = bdb_open_more(
            tmpname, db->dbenv->basedir, db->lrl, db->nix, db->ix_keylen,
            db->ix_dupes, db->ix_recnums, db->ix_datacopy, db->ix_collattr,
            db->ix_nullsallowed,
            db->numblobs + 1, /* one main record + the blobs blobs */
            db->dbenv->bdb_env, &bdberr);

        if (db->handle)
            logmsg(LOGMSG_INFO, "Found existing tempdb: %s, attempting to resume an in "
                   "progress schema change\n",
                   tmpname);
        else
           logmsg(LOGMSG_INFO, "Didn't find existing tempdb: %s, creating a new one\n",
                   tmpname);
    }

    if (!db->handle) /* did not/could not open existing one, creating new one */
    {
        db->handle =
            bdb_create(tmpname, db->dbenv->basedir, db->lrl, db->nix,
                       db->ix_keylen, db->ix_dupes, db->ix_recnums,
                       db->ix_datacopy, db->ix_collattr, db->ix_nullsallowed,
                       db->numblobs + 1, /* one main record + the blobs blobs */
                       db->dbenv->bdb_env, temp, &bdberr);
        if (db->handle == NULL) {
            logmsg(LOGMSG_ERROR, "%s: failed to open %s, rcode %d\n", __func__, tmpname,
                   bdberr);
            free(tmpname);
            return -1;
        }
    }

    /* clone the blobstripe genid.  this will definately be needed in the
     * future when we don't change genids on schema change, but right now
     * isn't really needed. */
    bdb_set_blobstripe_genid(db->handle, db->blobstripe_genid);
    free(tmpname);
    return 0;
}

/**
 * Verify a new schema change temporary db.  A newly created/resumed db should
 * have file versions that are all strictly greater than all of the original
 * db's file versions.
 * Schema change didn't used to delete new.tablename file versions from llmeta.
 * If a schema change failed before a newdb was created, the new master would
 * try to resume the sc and it could 'reopen' the temp db using old/stale
 * new.tablename file versions causing horrifying bugs.
 * @return returns 0 on success; !0 otherwise
 */
int verify_new_temp_sc_db(struct db *p_db, struct db *p_newdb)
{
    int i;
    int bdberr;
    unsigned long long db_max_file_version;
    unsigned long long newdb_min_file_version;

    /* find the db's smallest file version */

    db_max_file_version = 0;

    for (i = 0; i < (1 /*dta*/ + p_db->numblobs); ++i) {
        unsigned long long file_version;

        if (bdb_get_file_version_data(p_db->handle, NULL /*tran*/, i,
                                      &file_version, &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: bdb_get_file_version_index failed for db "
                            "data %d\n",
                    __func__, i);
            return -1;
        }

        if (sc_cmp_fileids(file_version, db_max_file_version) > 0)
            db_max_file_version = file_version;
    }

    for (i = 0; i < p_db->nix; ++i) {
        unsigned long long file_version;

        if (bdb_get_file_version_index(p_db->handle, NULL /*tran*/, i,
                                       &file_version, &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: bdb_get_file_version_index failed for db "
                            "index %d\n",
                    __func__, i);
            return -1;
        }

        if (sc_cmp_fileids(file_version, db_max_file_version) > 0)
            db_max_file_version = file_version;
    }

    /* find the newdb's smallest file version */

    newdb_min_file_version = ULLONG_MAX;

    for (i = 0; i < (1 /*dta*/ + p_newdb->numblobs); ++i) {
        unsigned long long file_version;

        if (bdb_get_file_version_data(p_newdb->handle, NULL /*tran*/, i,
                                      &file_version, &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: bdb_get_file_version_data failed for newdb "
                            "data %d\n",
                    __func__, i);
            return -1;
        }

        if (sc_cmp_fileids(file_version, newdb_min_file_version) < 0)
            newdb_min_file_version = file_version;
    }

    for (i = 0; i < p_newdb->nix; ++i) {
        unsigned long long file_version;

        if (bdb_get_file_version_index(p_newdb->handle, NULL /*tran*/, i,
                                       &file_version, &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: bdb_get_file_version_index failed for newdb "
                            "index %d\n",
                    __func__, i);
            return -1;
        }

        if (sc_cmp_fileids(file_version, newdb_min_file_version) < 0)
            newdb_min_file_version = file_version;
    }

    /* if the db has any file version >= any of newdb's file versions there has
     * been an error */
    if (sc_cmp_fileids(db_max_file_version, newdb_min_file_version) >= 0) {
        logmsg(LOGMSG_ERROR, "%s: db's max file version %#16llx >= newdb's min file "
                        "version %#16llx\n",
                __func__, db_max_file_version, newdb_min_file_version);
        return -1;
    }

    return 0;
}

/* close and remove the temp table after a failed schema change. */
int delete_temp_table(struct schema_change_type *s, struct db *newdb)
{
    int i, rc, bdberr;
    void *tran;
    struct ireq iq;

    rc = bdb_close_only(newdb->handle, &bdberr);
    if (rc) {
        sc_errf(s, "bdb_close_only rc %d bdberr %d\n", rc, bdberr);
        return -1;
    }

    init_fake_ireq(thedb, &iq);
    iq.usedb = newdb;
    rc = trans_start(&iq, NULL, &tran);
    if (rc) {
        sc_errf(s, "%d: trans_start rc %d\n", __LINE__, rc);
        return -1;
    }

    for (i = 0; i < 1000; i++) {
        if (!s->retry_bad_genids)
            sc_errf(s, "removing temp table for <%s>\n", newdb->dbname);
        if ((rc = bdb_del(newdb->handle, tran, &bdberr)) ||
            bdberr != BDBERR_NOERROR) {
            rc = -1;
            sc_errf(s, "%s: bdb_del failed with rc: %d bdberr: %d\n", __func__,
                    rc, bdberr);
        } else if ((rc = bdb_del_file_versions(newdb->handle, tran, &bdberr)) ||
                   bdberr != BDBERR_NOERROR) {
            rc = -1;
            sc_errf(s, "%s: bdb_del_file_versions failed with rc: %d bdberr: "
                       "%d\n",
                    __func__, rc, bdberr);
        }

        if (rc != 0) {
            trans_abort(&iq, tran);
            poll(NULL, 0, rand() % 100 + 1);
            rc = trans_start(&iq, NULL, &tran);
            if (rc) {
                sc_errf(s, "%d: trans_start rc %d\n", __LINE__, rc);
                return -1;
            }
        } else
            break;
    }
    if (rc != 0) {
        sc_errf(s, "Still failed to delete temp table for %s.  I am giving up "
                   "and going home.",
                newdb->dbname);
        return -1;
    }

    rc = trans_commit(&iq, tran, gbl_mynode);
    if (rc) {
        sc_errf(s, "%d: trans_commit rc %d\n", __LINE__, rc);
        return -1;
    }

    return 0;
}

int do_setcompr(struct ireq *iq, const char *rec, const char *blob)
{
    int rc;
    void *tran = NULL;
    if ((rc = trans_start(iq, NULL, &tran)) != 0) {
        sbuf2printf(iq->sb, ">%s -- trans_start rc:%d\n", __func__, rc);
        return rc;
    }

    struct db *db = iq->usedb;
    bdb_lock_table_write(db->handle, tran);
    int ra, ba;
    if ((rc = get_db_compress(db, &ra)) != 0)
        goto out;
    if ((rc = get_db_compress_blobs(db, &ba)) != 0)
        goto out;

    if (rec)
        ra = bdb_compr2algo(rec);
    if (blob)
        ba = bdb_compr2algo(blob);
    bdb_set_odh_options(db->handle, db->odh, ra, ba);
    if ((rc = put_db_compress(db, tran, ra)) != 0)
        goto out;
    if ((rc = put_db_compress_blobs(db, tran, ba)) != 0)
        goto out;
    if ((rc = trans_commit(iq, tran, gbl_mynode)) == 0) {
        logmsg(LOGMSG_USER, "%s -- TABLE:%s  REC COMP:%s  BLOB COMP:%s\n", __func__,
               db->dbname, bdb_algo2compr(ra), bdb_algo2compr(ba));
    } else {
        sbuf2printf(iq->sb, ">%s -- trans_commit rc:%d\n", __func__, rc);
    }
    tran = NULL;

    int bdberr = 0;
    if ((rc = bdb_llog_scdone(db->handle, setcompr, 1, &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR, "%s -- bdb_llog_scdone rc:%d bdberr:%d\n", __func__, rc,
                bdberr);
    }

out:
    if (tran) {
        trans_abort(iq, tran);
    }
    return rc;
}

int dryrun_int(struct schema_change_type *s, struct db *db, struct db *newdb,
               struct scinfo *scinfo)
{
    int changed;
    struct scplan plan;

    if (s->headers != db->odh)
        s->header_change = s->force_dta_rebuild = s->force_blob_rebuild = 1;

    if (scinfo->olddb_inplace_updates && !s->ip_updates && !s->force_rebuild) {
        sbuf2printf(s->sb,
                    ">Cannot remove inplace updates without rebuilding.\n");
        return -1;
    }

    if (scinfo->olddb_instant_sc && !s->instant_sc) {
        sbuf2printf(
            s->sb,
            ">Cannot remove instant schema-change without rebuilding.\n");
        return -1;
    }

    if (s->force_rebuild) {
        sbuf2printf(s->sb, ">Forcing table rebuild\n");
        goto out;
    }

    if (s->force_dta_rebuild) {
        sbuf2printf(s->sb, ">Forcing data file rebuild\n");
    }

    if (s->force_blob_rebuild) {
        sbuf2printf(s->sb, ">Forcing blob file rebuild\n");
    }

    if (verify_constraints_exist(NULL, newdb, newdb, s)) {
        return -1;
    }

    if (s->compress != scinfo->olddb_compress) {
        s->force_dta_rebuild = 1;
    }

    if (s->compress_blobs != scinfo->olddb_compress_blobs) {
        s->force_blob_rebuild = 1;
    }

    changed = ondisk_schema_changed(s->table, newdb, NULL, s);
    if (changed < 0) {
        if (changed == SC_BAD_NEW_FIELD) {
            sbuf2printf(s->sb,
                        ">Cannot add new field without dbstore or null\n");
            return -1;
        } else if (changed == SC_BAD_INDEX_CHANGE) {
            sbuf2printf(s->sb,
                        ">Cannot change index referenced by other tables\n");
            return -1;
        } else {
            sbuf2printf(s->sb, ">Failed to process schema!\n");
            return -1;
        }
    }

    if (create_schema_change_plan(s, db, newdb, &plan) != 0) {
        sbuf2printf(s->sb, ">Error in plan module.\n");
        sbuf2printf(s->sb, ">Will need to rebuild table\n");
        return 0;
    }

    if (changed == SC_NO_CHANGE) {
        if (db->n_constraints && newdb->n_constraints == 0) {
            sbuf2printf(s->sb, ">All table constraints will be dropped\n");
        } else {
            sbuf2printf(s->sb, ">There is no change in the schema\n");
        }
    } else if (db->version >= MAXVER && newdb->instant_schema_change) {
        sbuf2printf(s->sb, ">Table is at version: %d MAXVER: %d\n", db->version,
                    MAXVER);
        sbuf2printf(s->sb, ">Will need to rebuild table\n");
    }

out:
    print_schemachange_info(s, db, newdb);
    return 0;
}
