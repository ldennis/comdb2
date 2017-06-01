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

#include <memory_sync.h>
#include <autoanalyze.h>
#include <translistener.h>

#include "schemachange.h"
#include "sc_drop_table.h"
#include "sc_schema.h"
#include "sc_struct.h"
#include "sc_global.h"
#include "sc_logic.h"
#include "sc_csc2.h"
#include "sc_callbacks.h"
#include "sc_records.h"

static inline void set_implicit_options(struct schema_change_type *s,
                                        struct db *db, struct scinfo *scinfo)
{
    if (s->headers != db->odh)
        s->header_change = s->force_dta_rebuild = s->force_blob_rebuild = 1;
    if (s->compress != scinfo->olddb_compress)
        s->force_dta_rebuild = 1;
    if (s->compress_blobs != scinfo->olddb_compress_blobs)
        s->force_blob_rebuild = 1;
    if (scinfo->olddb_inplace_updates && !s->ip_updates)
        s->force_rebuild = 1;
    if (scinfo->olddb_instant_sc && !s->instant_sc)
        s->force_rebuild = 1;
}

static int delete_table(struct db *db, void * trans)
{
    remove_constraint_pointers(db);

    int rc, bdberr;
    if ((rc = bdb_close_only_tran(db->handle, trans, &bdberr))) {
        fprintf(stderr, "bdb_close_only rc %d bdberr %d\n", rc, bdberr);
        return -1;
    }

    char *table = db->dbname;
    delete_db(table);
    MEMORY_SYNC;
    delete_schema(table);
    bdb_del_table_csonparameters(trans, table);
    return 0;
}

int do_drop_table(struct ireq *iq, tran_type *tran)
{
    int rc;
    struct schema_change_type *s = iq->sc;
    int bdberr;
    struct db *db;
    struct db *newdb;
    int datacopy_odh = 0;
    iq->usedb = db = s->db = getdbbyname(s->table);
    if (db == NULL) {
        sc_errf(s, "Table doesn't exists\n");
        reqerrstr(iq, ERR_SC, "Table doesn't exists");
        return SC_TABLE_DOESNOT_EXIST;
    }
    if (db->n_rev_constraints > 0) {
        sc_errf(s, "Can't drop tables with foreign constraints\n");
        reqerrstr(iq, ERR_SC, "Can't drop tables with foreign constraints");
        return -1;
    }

    char new_prefix[32];
    int foundix;

    struct scinfo scinfo;

    set_schemachange_options_tran(s, db, &scinfo, tran);

    set_implicit_options(s, db, &scinfo);

    /*************************************************************************/
    extern int gbl_broken_max_rec_sz;
    int saved_broken_max_rec_sz = gbl_broken_max_rec_sz;
    if(s->db->lrl > COMDB2_MAX_RECORD_SIZE) {
        //we want to allow fastiniting and dropping this tbl
        gbl_broken_max_rec_sz = s->db->lrl - COMDB2_MAX_RECORD_SIZE;
    }

    if ((rc = load_db_from_schema(s, thedb, &foundix, NULL)))
        return rc;

    gbl_broken_max_rec_sz = saved_broken_max_rec_sz;
    /*************************************************************************/

    /* open a db using the loaded schema
     * TODO del NULL param, pre-llmeta holdover */
    db->sc_to = newdb = s->newdb =
        create_db_from_schema(thedb, s, db->dbnum, foundix, 1);
    if (newdb == NULL)
        return SC_INTERNAL_ERROR;

    if (add_cmacc_stmt(newdb, 1) != 0) {
        backout_schemas(newdb->dbname);
        sc_errf(s, "Failed to process schema\n");
        return -1;
    }

    /* hi please don't leak memory? */
    csc2_free_all();
    /**********************************************************************/
    /* force a "change" for a fastinit */
    schema_change = SC_TAG_CHANGE;

    /* Create temporary tables.  To try to avoid strange issues always
     * use a unqiue prefix.  This avoids multiple histories for these
     * new. files in our logs.
     *
     * Since the prefix doesn't matter and bdb needs to be able to unappend
     * it, we let bdb choose the prefix */
    /* ignore failures, there shouln't be any and we'd just have a
     * truncated prefix anyway */
    bdb_get_new_prefix(new_prefix, sizeof(new_prefix), &bdberr);

    rc = open_temp_db_resume(newdb, new_prefix, 0, 1, tran);
    if (rc) {
        /* TODO: clean up db */
        sc_errf(s, "Failed opening new db\n");
        change_schemas_recover(s->table);
        return -1;
    }

    /* we can resume sql threads at this point */

    /* Must do this before rebuilding, otherwise we'll have the wrong
     * blobstripe_genid. */
    transfer_db_settings(db, newdb);

    get_db_datacopy_odh_tran(db, &datacopy_odh, tran);
    if (s->fastinit || s->force_rebuild || /* we're first to set */
        newdb->instant_schema_change)      /* we're doing instant sc*/
    {
        datacopy_odh = 1;
    }

    /* we set compression /odh options in BDB ONLY here.
       For full operation they also need to be set in the meta tables.
       However the new db gets its meta table assigned further down,
       so we can't set meta options until we're there. */
    set_bdb_option_flags(newdb->handle, s->headers, s->ip_updates,
                         newdb->instant_schema_change, newdb->version,
                         s->compress, s->compress_blobs, datacopy_odh);

    /* set sc_genids, 0 them if we are starting a new schema change, or
     * restore them to their previous values if we are resuming */
    if (init_sc_genids(newdb, s)) {
        sc_errf(s, "Failed initializing sc_genids\n");
        /*trans_abort( &iq, transaction );*/
        /*live_sc_leave_exclusive_all(db->handle, transaction);*/
        delete_temp_table(iq, newdb);
        change_schemas_recover(s->table);
        return -1;
    }

    else {
        MEMORY_SYNC;
    }

    return SC_OK;
}

int finalize_drop_table(struct ireq *iq, tran_type *tran)
{
    struct schema_change_type *s = iq->sc;
    struct db *db = s->db;
    struct db *newdb = s->newdb;
    int rc;
    int bdberr = 0;
    int olddb_bthashsz;

    if (get_db_bthash_tran(db, &olddb_bthashsz, tran) != 0)
        olddb_bthashsz = 0;

    /* Before this handle is closed, lets wait for all the db reads to finish*/
    bdb_lock_table_write(db->handle, tran);

    rc = bdb_close_temp_state(newdb->handle, &bdberr);
    if (rc) {
        sc_errf(s, "Failed closing new db handle, bdberr\n", bdberr);
        return -1;
    } else
        sc_printf(s, "Close new db ok\n");
    /* at this point if a backup is going on, it will be bad */
    gbl_sc_commit_count++;

    /* load new csc2 data */
    rc = load_new_table_schema_tran(thedb, tran, s->table, s->newcsc2);
    if (rc != 0) {
        sc_errf(s, "Error loading new schema into meta tables, "
                   "trying again\n");
        return -1;
    }

    if ((rc = set_header_and_properties(tran, newdb, s, 1, olddb_bthashsz)))
        return -1;

    /*set all metapointers to new files*/
    rc = bdb_commit_temp_file_version_all(newdb->handle, tran, &bdberr);
    if (rc)
        return -1;

    /* delete any new file versions this table has */
    if (bdb_del_file_versions(newdb->handle, tran, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        sc_errf(s, "%s: bdb_del_file_versions failed\n", __func__);
        return -1;
    }

    if ((rc = mark_schemachange_over_tran(db->dbname, tran)))
        return rc;

    /* remove the new.NUM. prefix */
    bdb_remove_prefix(newdb->handle);

    /* TODO: need to free db handle - right now we just leak some memory */
    /* replace the old db definition with a new one */

    newdb->plan = NULL;

    delete_table(db, tran);
    /*Now that we don't have any data, please clear unwanted schemas.*/
    bdberr = bdb_reset_csc2_version(tran, db->dbname, db->version);
    if (bdberr != BDBERR_NOERROR)
        return -1;

    if ((rc = bdb_del(db->handle, tran, &bdberr)) != 0) {
        sc_errf(s, "%s: bdb_del failed with rc: %d bdberr: %d\n", __func__,
                rc, bdberr);
        return rc;
    } else if ((rc = bdb_del_file_versions(db->handle, tran, &bdberr))) {
        sc_errf(s, "%s: bdb_del_file_versions failed with rc: %d bdberr: "
                   "%d\n",
                __func__, rc, bdberr);
        return rc;
    }

    if (s->drop_table && (rc = table_version_upsert(db, tran, &bdberr)) != 0) {
        sc_errf(s, "Failed updating table version bdberr %d\n", bdberr);
        return rc;
    }

    if ((rc = llmeta_set_tables(tran, thedb)) != 0) {
        sc_errf(s, "Failed to set table names in low level meta\n");
        return rc;
    }

    if ((rc = create_sqlmaster_records(tran)) != 0) {
        sc_errf(s, "create_sqlmaster_records failed\n");
        return rc;
    }
    create_master_tables(); /* create sql statements */
 
    live_sc_off(db);

    /* delete files we don't need now */
    sc_del_unused_files_tran(db, tran);

    return 0;
}
