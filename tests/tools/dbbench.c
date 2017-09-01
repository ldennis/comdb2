#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <cdb2api.h>
#include <time.h>
#include <pthread.h>
#include <signal.h>
#include <poll.h>
#include <assert.h>
#include <errno.h>
#include "ts_diff.h"
#include "number_name.h"

#define MAXNUMNAME 128
#define NUMRECS_DEFAULT 3334
#define NUMTHDS_DEFAULT 30

static char *argv0=NULL;
unsigned long long tot_records_added=0;
void ilbyte_(void) 
{
    fprintf(stderr, "Dummy ilbyte_ called\n");
}

void usage(FILE *f)
{
    fprintf(f,"Usage: %s <cmd-line>\n", argv0);
    fprintf(f," -d <dbname>             - sets dbname\n");
    fprintf(f," -c <cltype>             - sets the cluster type (dev, alpha, beta, prod)\n");
    fprintf(f," -x <txnlevel>           - sets the transaction level\n");
    fprintf(f," -f <filename>           - sets results filename\n");
    fprintf(f," -A                      - run all dbbench tests in order w/ default values\n");
    fprintf(f," -G                      - run all dbbench tests with aggressive optimizations\n");
    fprintf(f,"\n");
    fprintf(f,"Flags for insert mode\n");
    fprintf(f," -I                      - set insert mode\n");
    fprintf(f," -T <numthds>            - sets the number of threads (defaults to %d)\n", NUMTHDS_DEFAULT);
    fprintf(f," -r <numrecs>            - sets the number of records (defaults to %d)\n", NUMRECS_DEFAULT);
    fprintf(f," -i <rows-in-insert>     - sets the number of rows in an insert (defaults to 1)\n");
    fprintf(f," -b                      - bind insert values (default)\n");
    fprintf(f," -u                      - don't bind insert values\n");
    fprintf(f," -z                      - each thread will insert in a single transaction\n");
    fprintf(f,"\n");
    fprintf(f,"Flags for delete mode\n");
    fprintf(f," -D                      - set delete mode\n");
    fprintf(f," -l <count>              - load with count records first\n");
    fprintf(f," -r <numrecs>            - set limit in where clause\n");
    fprintf(f,"\n");
    fprintf(f,"Flags for update mode\n");
    fprintf(f," -U                      - set update mode\n");
    fprintf(f," -l <count>              - load with count records first\n");
    fprintf(f," -m <max>                - set the update max\n");
    fprintf(f," -r <range>              - set the update range per statement\n");
    fprintf(f," -z                      - update records in a single transaction\n");
    fprintf(f,"\n");
    fprintf(f,"Flags for bulk-insert mode\n");
    fprintf(f," -E                      - set bulk-insert mode\n");
    fprintf(f," -i <rows-in-insert>     - set number of inserts per statement (defaults to 50)\n");
    fprintf(f," -r <numrecs>            - set number of insert statements (defaults to 2000)\n");
    fprintf(f," -u                      - don't bind insert values\n");
    fprintf(f," -z                      - insert record in a single transaction\n");
    fprintf(f,"\n");
    fprintf(f,"Flags for bulk-insert-union mode\n");
    fprintf(f," -B                      - set bulk-insert-union mode\n");
    fprintf(f," -i <rows-in-insert>     - set number of inserts per statement (defaults to 50)\n");
    fprintf(f," -r <numrecs>            - set number of insert statements (defaults to 2000)\n");
    fprintf(f," -u                      - don't bind insert values\n");
    fprintf(f," -z                      - insert record in a single transaction\n");
    fprintf(f,"\n");
    fprintf(f,"Flags for select mode\n");
    fprintf(f," -S                      - set select mode\n");
    fprintf(f," -m <max>                - set select max\n");
    fprintf(f," -r <range>              - set select range per statement\n");
    fprintf(f,"\n");
    fprintf(f,"Flags for select-like mode\n");
    fprintf(f," -L                      - set select-like mode\n");
    fprintf(f,"\n");
    fprintf(f,"Extraneous canned tests\n");
    fprintf(f," -s                      - single record insert per transaction\n");
    fprintf(f," -t                      - insert transaction test\n");
    fprintf(f," -X                      - insert big-transaction test\n");
    fprintf(f,"\n");
    fprintf(f,"Sundry flags\n");
    fprintf(f," -v                      - enable verbose mode\n");
    fprintf(f," -Q <testnum>            - run single alltests test\n");
    fprintf(f," -h                      - this menu\n");
    exit(1);
}

enum
{
    MODE_NONE
   ,MODE_INSERT
   ,MODE_DELETE
   ,MODE_UPDATE
   ,MODE_SELECT
   ,MODE_SELECT_LIKE
   ,MODE_BULK_INSERT
   ,MODE_BIUNION
   ,MODE_ALLTESTS
   ,MODE_1RECINS
   ,MODE_INSTXN
   ,MODE_BIGTXN
};

typedef struct results_struct
{
    int total_recs;
    unsigned long long total_usec;
    unsigned long long average_usec;
}
results_t;

typedef struct config_struct
{
    char            *dbname;
    int             override_mode;
    int             mode;
    int             alltests;
    int             aggressive;
    char            *cltype;
    char            *txnlevel;
    int             select_interval;
    int             select_max;
    int             rows_in_insert;
    int             setrowsininsert;
    int             numthreads;
    int             numrecs;
    int             setnumrecs;
    int             verbose;
    int             printivl;
    int             dobind;
    int             dotxn;
    int             loadcnt;
    int             minrand;
    int             maxrand;
    int             updmax;
    int             updrange;
}
config_t;

char *test_type_to_str(int type)
{
    switch(type)
    {
        case MODE_INSERT:
            return "insert";
            break;

        case MODE_DELETE:
            return "delete";
            break;

        case MODE_UPDATE:
            return "update";
            break;

        case MODE_SELECT:
            return "select";
            break;

        case MODE_SELECT_LIKE:
            return "select-like";
            break;

        case MODE_BULK_INSERT:
            return "bulk-insert";
            break;

        case MODE_BIUNION:
            return "bulk-insert-union";
            break;

        case MODE_1RECINS:
            return "single-record-insert";
            break;

        case MODE_INSTXN:
            return "insert-transaction";
            break;

        case MODE_BIGTXN:
            return "insert-big-transaction";
            break;

        default:
            return "(unknown)";
            break;
    }
}

void output_test_results(config_t *cfg, results_t *results, FILE *f)
{
    char *type=test_type_to_str(cfg->mode);
    char *txn=cfg->dotxn?"-transaction":"";
    char *parallel=cfg->numthreads>1?"-parallel":"";
    char testname[64];
    snprintf(testname, sizeof(testname), "%s%s%s", type, parallel, txn);
    fprintf(f,"%-*s %*llu records-per-second\n", 36, testname, 7, (1000000 / results->average_usec));
}

static config_t *default_config(void)
{
    config_t *c=calloc(1, sizeof(config_t));
    c->dbname=NULL;
    c->mode=MODE_NONE;
    c->cltype="default";
    c->txnlevel=NULL;
    c->rows_in_insert=1;
    c->setrowsininsert=0;
    c->numrecs=NUMRECS_DEFAULT;
    c->numthreads=NUMTHDS_DEFAULT;
    c->setnumrecs=0;
    c->loadcnt=0;
    c->minrand=1;
    c->maxrand=99999999;
    c->verbose=0;
    c->printivl=10;
    c->updmax=100000;
    c->select_interval=2000;
    c->select_max=100000;
    c->dotxn=0;
    c->aggressive=0;
    c->dobind=1;
    return c;
}

enum
{
    ALLTESTS_AGGRESSIVE                 = 0x00000001
   ,ALLTESTS_VERBOSE                    = 0x00000002
};

static config_t *insert_parallel_config(char *dbname, uint32_t flags)
{
    config_t *c=default_config();
    c->dbname=dbname;
    c->mode=MODE_INSERT;
    c->verbose=(flags & ALLTESTS_VERBOSE)?1:0;
    c->numrecs=3334;
    c->numthreads=30;

    /* All over the map above 500 */
    if(flags & ALLTESTS_AGGRESSIVE)
    {
        c->rows_in_insert=341;
        c->dobind=1;
    }
    else
    {
        c->rows_in_insert=1;
        c->dobind=0;
    }
    c->alltests=1;
    return c;
}

static config_t *delete_config(char *dbname, uint32_t flags)
{
    config_t *c=default_config();
    c->dbname=dbname;
    c->mode=MODE_DELETE;
    c->verbose=(flags & ALLTESTS_VERBOSE)?1:0;

    /* This is a 'delete where 1', so nothing to bind */
    if(flags & ALLTESTS_AGGRESSIVE)
    {
        c->numrecs=10000;
        c->dobind=0;
    }
    else
    {
        c->numrecs=5000;
        c->dobind=0;
    }
    c->alltests=1;
    return c;
}

static config_t *insert_transaction_parallel_config(char *dbname, uint32_t flags)
{
    config_t *c=default_config();
    c->dbname=dbname;
    c->mode=MODE_INSERT;
    c->verbose=(flags & ALLTESTS_VERBOSE)?1:0;
    c->numrecs=3334;
    c->numthreads=30;

    if(flags & ALLTESTS_AGGRESSIVE)
    {
        c->rows_in_insert=3334;
        c->dobind=0;
    }
    else
    {
        c->rows_in_insert=1;
        c->dobind=0;
    }

    c->rows_in_insert=c->numrecs;
    c->dotxn=1;
    c->alltests=1;
    return c;
}

static config_t *insert_config(char *dbname, uint32_t flags)
{
    config_t *c=default_config();
    c->dbname=dbname;
    c->mode=MODE_INSERT;
    c->verbose=(flags & ALLTESTS_VERBOSE)?1:0;
    c->numrecs=100000;
    c->numthreads=1;
    if(flags & ALLTESTS_AGGRESSIVE)
    {
        c->rows_in_insert=1;
        c->dobind=0;
    }
    else
    {
        c->rows_in_insert=1;
        c->dobind=0;
    }
    c->alltests=1;
    return c;
}

static config_t *insert_big_transaction_config(char *dbname, uint32_t flags)
{
    config_t *c=default_config();
    c->dbname=dbname;
    c->mode=MODE_INSERT;
    c->verbose=(flags & ALLTESTS_VERBOSE)?1:0;
    c->numthreads=1;
    c->dobind=0;
    c->dotxn=1;
    c->alltests=1;
    return c;
}

static config_t *insert_transaction_config(char *dbname, uint32_t flags)
{
    config_t *c=default_config();
    c->dbname=dbname;
    c->mode=MODE_INSERT;
    c->verbose=(flags & ALLTESTS_VERBOSE)?1:0;
    c->numrecs=100000;
    c->numthreads=1;
    if(flags & ALLTESTS_AGGRESSIVE)
    {
        c->rows_in_insert=c->numrecs;
        c->dobind=0;
    }
    else
    {
        c->rows_in_insert=1;
        c->dobind=0;
    }
    c->dotxn=1;
    c->alltests=1;
    return c;
}

static config_t *update_config(char *dbname, uint32_t flags)
{
    config_t *c=default_config();
    c->dbname=dbname;
    c->mode=MODE_UPDATE;
    c->verbose=(flags & ALLTESTS_VERBOSE)?1:0;
    c->numrecs=100;
    if(flags & ALLTESTS_AGGRESSIVE)
    {
        c->dobind=1;
    }
    else
    {
        c->dobind=0;
    }
    c->alltests=1;
    return c;
}

static config_t *update_transaction_config(char *dbname, uint32_t flags)
{
    config_t *c=default_config();
    c->dbname=dbname;
    c->mode=MODE_UPDATE;
    c->verbose=(flags & ALLTESTS_VERBOSE)?1:0;
    c->numrecs=100;
    c->dotxn=1;
    c->alltests=1;
    return c;
}

static config_t *select_config(char *dbname, uint32_t flags)
{
    config_t *c=default_config();
    c->dbname=dbname;
    c->mode=MODE_SELECT;
    c->verbose=(flags & ALLTESTS_VERBOSE)?1:0;
    if(flags & ALLTESTS_AGGRESSIVE)
    {
        c->dobind=1;
    }
    else
    {
        c->dobind=0;
    }
    c->alltests=1;
    return c;
}

static config_t *select_like_config(char *dbname, uint32_t flags)
{
    config_t *c=default_config();
    c->dbname=dbname;
    c->mode=MODE_SELECT_LIKE;
    c->verbose=(flags & ALLTESTS_VERBOSE)?1:0;
    c->dobind=0;
    c->alltests=1;
    return c;
}

static config_t *bulk_insert_config(char *dbname, uint32_t flags)
{
    config_t *c=default_config();
    c->dbname=dbname;
    c->mode=MODE_BULK_INSERT;
    c->verbose=(flags & ALLTESTS_VERBOSE)?1:0;
    c->numthreads=1;
    c->numrecs=100000;
    c->rows_in_insert=100000;
    c->dobind=0;
    c->alltests=1;
    return c;
}

static config_t *bulk_insert_transaction_config(char *dbname, uint32_t flags)
{
    config_t *c=default_config();
    c->dbname=dbname;
    c->mode=MODE_BULK_INSERT;
    c->verbose=(flags & ALLTESTS_VERBOSE)?1:0;
    c->numthreads=1;
    c->numrecs=100000;
    c->rows_in_insert=100000;
    c->dobind=0;
    c->dotxn=1;
    c->alltests=1;
    return c;
}

static config_t *bulk_insert_union_config(char *dbname, uint32_t flags)
{
    config_t *c=default_config();
    c->dbname=dbname;
    c->mode=MODE_BIUNION;
    c->verbose=(flags & ALLTESTS_VERBOSE)?1:0;
    c->numrecs=100000;
    c->rows_in_insert=50;
    c->alltests=1;
    return c;
}

static config_t *bulk_insert_union_transaction_config(char *dbname, uint32_t flags)
{
    config_t *c=default_config();
    c->dbname=dbname;
    c->mode=MODE_BIUNION;
    c->verbose=(flags & ALLTESTS_VERBOSE)?1:0;
    c->numrecs=100000;
    c->rows_in_insert=50;
    c->dotxn=1;
    c->alltests=1;
    return c;
}

typedef struct insert_val
{
    long long               a;
    long long               r;
    char                    *number_name;
}
insert_val_t;

typedef struct insert_statement
{
    char                    *sql;
    int                     count;
    int                     dobind;
    insert_val_t            *ival;
}
insert_statement_t;

typedef struct thread_work
{
    pthread_t               tid;
    cdb2_hndl_tp            *hndl;
    config_t                *cfg;
    insert_statement_t      *in;
    int                     dotxn;
    int                     icnt;
}
thread_work_t;

static long long getrand(config_t *cfg)
{
    long long range=(cfg->maxrand-cfg->minrand);
    long long r=(rand()%range);
    return r+cfg->minrand;
}

int fastinit(config_t *cfg)
{
    char sql[256];
    int rc;
    cdb2_hndl_tp *hndl;
    char config[80];
    snprintf(config, sizeof(config), "%s.cfg", cfg->dbname);
    cdb2_set_comdb2db_config(config);
    if((rc=cdb2_open(&hndl, cfg->dbname, cfg->cltype, 1))!=0)
    {
        fprintf(stderr, "Error allocating handle, %s\n", cdb2_errstr(hndl));
        exit(1);
    }
    snprintf(sql, sizeof(sql), "truncate tix");

    if((rc=cdb2_run_statement(hndl, sql))!=0)
    {
        fprintf(stderr, "Error setting transaction level, %s\n", cdb2_errstr(hndl));
        exit(1);
    }
    do
    {
        rc=cdb2_next_record(hndl);
    }
    while(rc == CDB2_OK);

    return 0;
}

static int lastpr;

/* Keep track of the current length */
char *strncatend(char *dest, const char *src, size_t *curoff, size_t n)
{
    if(*curoff >= n)
        return dest;

    if(dest[*curoff]!='\0')
    {
        *curoff=strlen(dest);
    }

    if(*curoff >= n)
        return dest;

    strncat(&dest[*curoff], src, n - *curoff);
    *curoff += strlen(src);

    return dest;
}

void *insert_thread(void *arg)
{
    thread_work_t *tw=(thread_work_t *)arg;
    cdb2_effects_tp effects;
    int now, dobind=tw->cfg->dobind;
    char sql[80];
    insert_val_t *iv;
    int i,j,k,rc;

    if(dobind)
    {
        iv=(insert_val_t *)calloc(tw->cfg->rows_in_insert, sizeof(insert_val_t));

        for(i=0;i<tw->cfg->rows_in_insert;i++)
        {
            cdb2_bind_index(tw->hndl, (i*3)+1, CDB2_INTEGER, &iv[i].a, sizeof(iv[i].a));
            cdb2_bind_index(tw->hndl, (i*3)+2, CDB2_INTEGER, &iv[i].r, sizeof(iv[i].r));
            iv[i].number_name=(char *)calloc(1, MAXNUMNAME);
            cdb2_bind_index(tw->hndl, (i*3)+3, CDB2_CSTRING, 
                    iv[i].number_name,MAXNUMNAME);
        }
    }
    else
    {
        iv=NULL;
    }

    if(tw->dotxn)
    {
        snprintf(sql, sizeof(sql), "BEGIN");

        if((rc=cdb2_run_statement(tw->hndl, sql))!=0)
        {
            fprintf(stderr, "error running begin,  '%s'\n", cdb2_errstr(tw->hndl));
            exit(1);
        }
        do
        {
            rc = cdb2_next_record(tw->hndl);
        }
        while(rc == CDB2_OK);

        if(rc != CDB2_OK_DONE)
        {
            fprintf(stderr, "Error running begin, %s\n", cdb2_errstr(tw->hndl));
            exit(1);
        }
    }

    /* Number of insert statements */
    for(i=0;i<tw->icnt;i++)
    {
        insert_statement_t *in=&tw->in[i];

        for(j=0;dobind && j<in->count;j++)
        {
            /* Set bound values */
            iv[j].a=in->ival[j].a;
            iv[j].r=in->ival[j].r;
            strncpy(iv[j].number_name, in->ival[j].number_name, MAXNUMNAME);
        }

        /* Execute sql */
        if((rc=cdb2_run_statement(tw->hndl, in->sql))!=0)
        {
            fprintf(stderr, "Insert error rc=%d, %s\n", rc, 
                    cdb2_errstr(tw->hndl));
            exit(1);
        }

        if(!tw->dotxn && (rc = cdb2_get_effects(tw->hndl, &effects))!=0)
        {
            fprintf(stderr, "Error calling get_effects, %s\n", cdb2_errstr(tw->hndl));
            exit(1);
        }

        tot_records_added+=in->count;
        if(tw->cfg->verbose && ((now=time(NULL)) - lastpr) >= tw->cfg->printivl)
        {
            lastpr=now;
            fprintf(stderr, "Added %llu records\n", tot_records_added);
        }
    }

    if(tw->dotxn)
    {
        snprintf(sql, sizeof(sql), "COMMIT");

        if((rc=cdb2_run_statement(tw->hndl, sql))!=0)
        {
            fprintf(stderr, "error running commit,  '%s'\n", cdb2_errstr(tw->hndl));
            exit(1);
        }

        if((rc = cdb2_get_effects(tw->hndl, &effects))!=0)
        {
            fprintf(stderr, "Error calling get_effects, %s\n", cdb2_errstr(tw->hndl));
            exit(1);
        }
    }

    /* Cleanup now if we're running all tests */
    if(dobind && tw->cfg->alltests)
    {
        for(i=0;i<tw->cfg->rows_in_insert;i++)
        {
            free(iv[i].number_name);
        }
        free(iv);
    }

    return NULL;
}

int insert_test(config_t *cfg, FILE *f)
{
    int i, j, k, rc, lastcnt, sqlsz;
    struct timespec start, end, elapsed;
    results_t results;
    unsigned long long nsecs, usecs, totrecs, average_nsec;
    char config[80];
    thread_work_t *twork=calloc(cfg->numthreads, sizeof(thread_work_t));
    tot_records_added = 0;

    if(cfg->mode == MODE_INSERT && cfg->numrecs % cfg->rows_in_insert)
    {
        cfg->numrecs += cfg->rows_in_insert - (cfg->numrecs % cfg->rows_in_insert);
        if(cfg->verbose)
        {
            fprintf(stderr, "Rows per insert must divide num-records\n");
            fprintf(stderr, "Increased numrecs to %d\n",  cfg->numrecs);
        }
    }

    snprintf(config, sizeof(config), "%s.cfg", cfg->dbname);
    cdb2_set_comdb2db_config(config);

    for(i=0;i<cfg->numthreads;i++)
    {
        thread_work_t *tw=&(twork[i]);
        char sql[256];
        int isfirst, maxival=(cfg->numrecs/cfg->rows_in_insert);
        lastcnt=i;

        tw->cfg=cfg;
        tw->dotxn=cfg->dotxn;
        tw->in=(insert_statement_t *)calloc(maxival, sizeof(insert_statement_t));
        tw->icnt=maxival;

        if((rc=cdb2_open(&tw->hndl, cfg->dbname, cfg->cltype, 1))!=0)
        {
            fprintf(stderr, "Error allocating handle, %s\n", cdb2_errstr(tw->hndl));
            exit(1);
        }

        if(cfg->txnlevel)
        {
            snprintf(sql, sizeof(sql), "SET TRANSACTION %s\n", cfg->txnlevel);
            if((rc=cdb2_run_statement(tw->hndl, sql))!=0)
            {
                fprintf(stderr, "Error setting transaction level, %s\n", cdb2_errstr(tw->hndl));
                exit(1);
            }
            do
            {
                rc=cdb2_next_record(tw->hndl);
            }
            while(rc == CDB2_OK);
        }

        for(j=0;j<maxival;j++)
        {
            int fudgemult=(!cfg->dobind) ? (MAXNUMNAME + 16) : 0;
            size_t curoff = 0;
            char tmpsql[150];
            insert_statement_t *in=&(tw->in[j]);
            in->count=cfg->rows_in_insert;

            /* Allocate sql statement */
            in->dobind=cfg->dobind;
            in->sql=(char *)malloc(sqlsz=(64 + ((128 + 8 + 8 + fudgemult) * cfg->rows_in_insert)));
            snprintf(in->sql, sqlsz, "INSERT INTO tix VALUES ");
            isfirst=1;

            /* Create the sql statement */
            for(k=0;k<in->count;k++)
            {
                if(isfirst==0)
                    strncatend(in->sql, ",", &curoff, sqlsz);
                if(in->dobind)
                {
                    strncatend(in->sql, "(?,?,?)", &curoff, sqlsz);
                }
                else
                {
                    long long r=getrand(cfg);
                    snprintf(tmpsql, sizeof(tmpsql), "(%d,%lld,'%s')", lastcnt, r, number_name(r));
                    strncatend(in->sql, tmpsql, &curoff, sqlsz);
                    lastcnt+=cfg->numthreads;
                }
                isfirst=0;
            }

            if(in->dobind)
            {
                /* Allocate insert */
                in->ival=(insert_val_t *)calloc(in->count, sizeof(insert_val_t));

                /* Fill this in */
                for(k=0;k<in->count;k++)
                {
                    insert_val_t *iv=&in->ival[k];
                    iv->a=lastcnt;
                    iv->r=getrand(cfg);
                    iv->number_name=number_name(iv->r);
                    lastcnt+=cfg->numthreads;
                }
            }
            else
            {
                in->ival=NULL;
            }
        }
    }

    /* Start timer */
    clock_gettime(CLOCK_REALTIME, &start);

    if(cfg->numthreads == 1)
    {
        insert_thread(&twork[0]);
    }
    else
    {
        for(i=0;i<cfg->numthreads;i++)
        {
            thread_work_t *tw=&(twork[i]);
            if((rc=pthread_create(&tw->tid,NULL,insert_thread,tw))!=0)
            {
                perror("Pthread create\n");
                exit(1);
            }
        }

        for(i=0;i<cfg->numthreads;i++)
        {
            thread_work_t *tw=&(twork[i]);
            pthread_join(tw->tid,NULL);
        }
    }

    /* Stop timer */
    clock_gettime(CLOCK_REALTIME, &end);

    ts_diff(&elapsed, &start, &end);

    nsecs=elapsed.tv_sec;
    nsecs*=1000000000;
    nsecs+=elapsed.tv_nsec;

    /* total records */
    totrecs=cfg->numrecs*cfg->numthreads;

    /* records per second */
    average_nsec = nsecs / totrecs;

    results.total_recs = totrecs;
    results.total_usec = (nsecs / 1000);
    results.average_usec = (average_nsec / 1000);

    output_test_results(cfg, &results, f);

    /* Close the handles */
    for(i=0;cfg->alltests && i<cfg->numthreads;i++)
    {
        thread_work_t *tw=&(twork[i]);
        int maxival=(cfg->numrecs/cfg->rows_in_insert);

        for(j=0;j<maxival;j++)
        {
            insert_statement_t *in=&(tw->in[j]);
            free(in->sql);
            if(in->ival) free(in->ival);
        }
        cdb2_close(tw->hndl);
    }
    if(cfg->alltests) free(twork);
    return 0;
}

int delete_test(config_t *cfg, FILE *f)
{
    int rc, i, j;
    char sql[256], sqltxn[80],config[80];
    cdb2_hndl_tp *hndl;
    cdb2_effects_tp effects={0};
    results_t results;
    struct timespec start, end, elapsed;
    unsigned long long nsecs, usecs, totrecs, average_nsec, delrecs=0;

    cfg->numthreads=1;

    snprintf(config, sizeof(config), "%s.cfg", cfg->dbname);
    cdb2_set_comdb2db_config(config);

    if((rc=cdb2_open(&hndl, cfg->dbname, cfg->cltype, 1))!=0)
    {
        fprintf(stderr, "Error allocating handle, %s\n", cdb2_errstr(hndl));
        exit(1);
    }

    if(cfg->txnlevel)
    {
        snprintf(sql, sizeof(sql), "SET TRANSACTION %s\n", cfg->txnlevel);
        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "Error setting transaction level, %s\n", cdb2_errstr(hndl));
            exit(1);
        }
        do
        {
            rc=cdb2_next_record(hndl);
        }
        while(rc == CDB2_OK);
    }
    
    /* If we should load this, do it in a single thread */
    if(cfg->loadcnt)
    {
        int loadfactor=500, sqlsz;
        char *loadsql = malloc(sqlsz=(128 + (loadfactor * (MAXNUMNAME + 16))));
        fprintf(stderr, "Loading table tix with %d records\n", cfg->loadcnt);

        snprintf(sql, sizeof(sql), "BEGIN");
        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "error running begin,  '%s'\n", cdb2_errstr(hndl));
            exit(1);
        }
        do
        {
            rc = cdb2_next_record(hndl);
        }
        while(rc == CDB2_OK);

        for(i=0;i<cfg->loadcnt;i+=loadfactor)
        {
            int isfirst=1, remaining = (cfg->loadcnt - i);
            size_t curoff=0;
            char tmpsql[150];

            snprintf(loadsql, sqlsz, "INSERT INTO tix VALUES ");

            if(remaining > loadfactor) 
                remaining = loadfactor;

            isfirst=1;

            for(j=0;j<remaining;j++)
            {
                long long rnd=getrand(cfg);

                if(isfirst==0)
                    strncatend(loadsql, ",", &curoff, sqlsz);

                snprintf(tmpsql, sizeof(tmpsql), "(%d,%lld,'%s')", (i*loadfactor)+j, rnd, number_name(rnd));
                strncatend(loadsql, tmpsql, &curoff, sqlsz);
                isfirst=0;
            }

            /* Execute sql */
            if((rc=cdb2_run_statement(hndl, loadsql))!=0)
            {
                fprintf(stderr, "Insert error rc=%d, %s\n", rc, 
                        cdb2_errstr(hndl));
                exit(1);
            }

        }
        snprintf(sql, sizeof(sql), "COMMIT");

        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "error running commit,  '%s'\n", cdb2_errstr(hndl));
            exit(1);
        }

        do
        {
            rc=cdb2_next_record(hndl);
        }
        while(rc == CDB2_OK);
        fprintf(stderr, "Loaded!  Running delete test.\n");
        free(loadsql);
    }

    if(cfg->numrecs>0)
    {
        snprintf(sql, sizeof(sql), "DELETE from tix WHERE 1 limit %d", cfg->numrecs);
    }
    else
    {
        snprintf(sql, sizeof(sql), "DELETE from tix WHERE 1");
    }

    /* Start timer */
    clock_gettime(CLOCK_REALTIME, &start);

    do
    {
        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "Error runing delete statement, %s\n", cdb2_errstr(hndl));
            exit(1);
        }

        if((rc=cdb2_get_effects(hndl, &effects))!=0)
        {
            fprintf(stderr, "Error calling get_effects, %s\n", cdb2_errstr(hndl));
            exit(1);
        }

        delrecs+=effects.num_deleted;
    }
    while(cfg->numrecs != 0 && effects.num_deleted == cfg->numrecs);

    /* Stop timer */
    clock_gettime(CLOCK_REALTIME, &end);

    ts_diff(&elapsed, &start, &end);

    nsecs=elapsed.tv_sec;
    nsecs*=1000000000;
    nsecs+=elapsed.tv_nsec;

    /* total records */
    totrecs=delrecs;

    /* records per second */
    average_nsec = nsecs / totrecs;

    results.total_recs = delrecs;
    results.total_usec = (nsecs / 1000);
    results.average_usec = (average_nsec / 1000);

    output_test_results(cfg, &results, f);

    cdb2_close(hndl);

    return 0;
}

int update_test(config_t *cfg, FILE *f)
{
    int rc, i, j, startnum, endnum, countloop=0;
    results_t results;
    char sql[256], config[80];
    cdb2_hndl_tp *hndl;
    cdb2_effects_tp effects;
    struct timespec start, end, elapsed;
    unsigned long long nsecs, usecs, totrecs, tot_records_updated=0, average_nsec, delrecs=0;

    snprintf(config, sizeof(config), "%s.cfg", cfg->dbname);
    cdb2_set_comdb2db_config(config);

    cfg->numthreads=1;

    if((rc=cdb2_open(&hndl, cfg->dbname, cfg->cltype, 1))!=0)
    {
        fprintf(stderr, "Error allocating handle, %s\n", cdb2_errstr(hndl));
        exit(1);
    }

    if(cfg->txnlevel)
    {
        snprintf(sql, sizeof(sql), "SET TRANSACTION %s\n", cfg->txnlevel);
        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "Error setting transaction level, %s\n", cdb2_errstr(hndl));
            exit(1);
        }
        do
        {
            rc=cdb2_next_record(hndl);
        }
        while(rc == CDB2_OK);
    }

    /* If we should load this, do it in a single thread */
    if(cfg->loadcnt)
    {
        int loadfactor=500, sqlsz;
        char *loadsql = malloc(sqlsz=(128 + (loadfactor * (MAXNUMNAME + 16))));
        fprintf(stderr, "Loading table tix with %d records\n", cfg->loadcnt);

        snprintf(sql, sizeof(sql), "BEGIN");
        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "error running begin,  '%s'\n", cdb2_errstr(hndl));
            exit(1);
        }
        do
        {
            rc = cdb2_next_record(hndl);
        }
        while(rc == CDB2_OK);

        for(i=0;i<cfg->loadcnt;i++)
        {
            int isfirst=1, remaining = (cfg->loadcnt - i);
            size_t curoff=0;
            char tmpsql[150];

            snprintf(sql, sizeof(sql), "INSERT INTO tix VALUES ");

            if(remaining > loadfactor) 
                remaining = loadfactor;

            isfirst=1;

            for(j=0;j<remaining;j++)
            {
                long long rnd=getrand(cfg);

                if(isfirst==0)
                    strncatend(loadsql, ",", &curoff, sqlsz);

                snprintf(tmpsql, sizeof(tmpsql), "(%d,%lld,'%s')", (i*loadfactor)+j, rnd, number_name(rnd));
                strncatend(loadsql, tmpsql, &curoff, sqlsz);
                isfirst=0;
            }

            /* Execute sql */
            if((rc=cdb2_run_statement(hndl, loadsql))!=0)
            {
                fprintf(stderr, "Insert error rc=%d, %s\n", rc, 
                        cdb2_errstr(hndl));
                exit(1);
            }
        }

        snprintf(sql, sizeof(sql), "COMMIT");

        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "error running commit,  '%s'\n", cdb2_errstr(hndl));
            exit(1);
        }

        do
        {
            rc = cdb2_next_record(hndl);
        }
        while(rc == CDB2_OK);
        fprintf(stderr, "Loaded!  Running update test.\n");
        free(loadsql);
    }


    /* Start timer */
    clock_gettime(CLOCK_REALTIME, &start);


    if(cfg->dotxn)
    {
        snprintf(sql, sizeof(sql), "BEGIN");

        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "error running begin,  '%s'\n", cdb2_errstr(hndl));
            exit(1);
        }
        do
        {
            rc = cdb2_next_record(hndl);
        }
        while(rc == CDB2_OK);

        if(rc != CDB2_OK_DONE)
        {
            fprintf(stderr, "Error running begin, %s\n", cdb2_errstr(hndl));
            exit(1);
        }
    }

    if(cfg->dobind)
    {
        snprintf(sql, sizeof(sql), "UPDATE tix set b=b*2 WHERE a>=? and a<?");

        cdb2_bind_index(hndl, 1, CDB2_INTEGER, &startnum, sizeof(startnum));
        cdb2_bind_index(hndl, 2, CDB2_INTEGER, &endnum, sizeof(endnum));
    }

    for(startnum=0; startnum < cfg->updmax; startnum+=cfg->numrecs)
    {
        endnum=startnum+cfg->numrecs;

        if(!cfg->dobind)
        {
            snprintf(sql, sizeof(sql), "UPDATE tix set b=b*2 WHERE a>=%d and a<%d", startnum, endnum);
        }

        /* Execute sql */
        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "Update error rc=%d, %s\n", rc, 
                    cdb2_errstr(hndl));
            exit(1);
        }

        countloop++;

        if(!cfg->dotxn) 
        {
            if((rc = cdb2_get_effects(hndl, &effects))!=0)
            {
                fprintf(stderr, "Error calling get_effects, %s\n", cdb2_errstr(hndl));
                exit(1);
            }
            tot_records_updated+=effects.num_updated;
        }
    }

    if(cfg->dotxn)
    {
        snprintf(sql, sizeof(sql), "COMMIT");

        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "error running commit,  '%s'\n", cdb2_errstr(hndl));
            exit(1);
        }

        if((rc = cdb2_get_effects(hndl, &effects))!=0)
        {
            fprintf(stderr, "Error calling get_effects, %s\n", cdb2_errstr(hndl));
            exit(1);
        }

        tot_records_updated+=effects.num_updated;
    }

    /* Stop timer */
    clock_gettime(CLOCK_REALTIME, &end);

    ts_diff(&elapsed, &start, &end);

    nsecs=elapsed.tv_sec;
    nsecs*=1000000000;
    nsecs+=elapsed.tv_nsec;

    /* total records */
    totrecs=tot_records_updated;

    /* records per second */
    average_nsec = nsecs / totrecs;

    results.total_recs = totrecs;
    results.total_usec = (nsecs / 1000);
    results.average_usec = (average_nsec / 1000);

    output_test_results(cfg, &results, f);

    cdb2_close(hndl);
    return 0;
}

int select_test(config_t *cfg, FILE *f)
{
    int rc, rc2, i, j, sqlsz, k, maxival=(cfg->numrecs/cfg->rows_in_insert), lastidx=0, now, startnum, endnum, countloop=0;
    results_t results;
    insert_val_t *iv;
    char sql[256], sqltxn[80],config[80];
    cdb2_hndl_tp *hndl;
    cdb2_effects_tp effects;
    int isfirst;
    struct timespec start, end, elapsed;
    unsigned long long nsecs, totrecs, average_nsec, totcount=0;

    cfg->numthreads=1;

    snprintf(config, sizeof(config), "%s.cfg", cfg->dbname);
    cdb2_set_comdb2db_config(config);

    if((rc=cdb2_open(&hndl, cfg->dbname, cfg->cltype, 1))!=0)
    {
        fprintf(stderr, "Error allocating handle, %s\n", cdb2_errstr(hndl));
        exit(1);
    }

    /* Set sql */
    if(cfg->dobind)
    {
        snprintf(sql, sizeof(sql), "SELECT count(*), avg(b) FROM tix WHERE a>=? AND a<?");
        cdb2_bind_index(hndl, 1, CDB2_INTEGER, &startnum, sizeof(startnum));
        cdb2_bind_index(hndl, 2, CDB2_INTEGER, &endnum, sizeof(endnum));
    }

    /* Start timer */
    clock_gettime(CLOCK_REALTIME, &start);

    if(cfg->dotxn)
    {
        snprintf(sqltxn, sizeof(sqltxn), "BEGIN");

        if((rc=cdb2_run_statement(hndl, sqltxn))!=0)
        {
            fprintf(stderr, "error running begin,  '%s'\n", cdb2_errstr(hndl));
            exit(1);
        }
        do
        {
            rc = cdb2_next_record(hndl);
        }
        while(rc == CDB2_OK);

        if(rc != CDB2_OK_DONE)
        {
            fprintf(stderr, "Error running begin, %s\n", cdb2_errstr(hndl));
            exit(1);
        }
    }

    for(startnum=0;startnum<cfg->select_max;startnum+=cfg->select_interval)
    {
        endnum=startnum+cfg->select_interval;

        if(!cfg->dobind)
        {
            snprintf(sql, sizeof(sql), 
                    "SELECT count(*), avg(b) FROM tix WHERE a>=%d AND a<%d", 
                    startnum, endnum);
        }

        /* Execute sql */
        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "Select error rc=%d, %s\n", rc, 
                    cdb2_errstr(hndl));
            exit(1);
        }

        countloop++;

        do
        {
            rc2 = cdb2_next_record(hndl);
            if(CDB2_OK == rc2)
            {
                int numcols=2, type=CDB2_INTEGER;
                int *icountp;
                long long *llcountp;

                if(numcols!=2) abort();

                /* Add the count to the total number of records found */
                numcols=cdb2_numcolumns(hndl);
                type=cdb2_column_type(hndl, 0);

                if(type != CDB2_INTEGER) abort();

                llcountp=cdb2_column_value(hndl, 0);
                if(llcountp) totcount+=(*llcountp);
            }
        }
        while(rc2 == CDB2_OK);
    }

    if(cfg->dotxn)
    {
        snprintf(sql, sizeof(sql), "COMMIT");

        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "error running commit,  '%s'\n", cdb2_errstr(hndl));
            exit(1);
        }

        if((rc = cdb2_get_effects(hndl, &effects))!=0)
        {
            fprintf(stderr, "Error calling get_effects, %s\n", cdb2_errstr(hndl));
            exit(1);
        }
    }

    /* Stop timer */
    clock_gettime(CLOCK_REALTIME, &end);

    /* Calculate insert time */
    ts_diff(&elapsed, &start, &end);
    nsecs=elapsed.tv_sec;
    nsecs*=1000000000;
    nsecs+=elapsed.tv_nsec;

    totrecs=totcount;

    /* records per second */
    average_nsec = nsecs / totrecs;

    results.total_recs=totrecs;
    results.total_usec = (nsecs / 1000);
    results.average_usec = (average_nsec / 1000);

    output_test_results(cfg, &results, f);

    cdb2_close(hndl);
    return 0;
}

int select_like_test(config_t *cfg, FILE *f)
{
    int rc, rc2, i, countloop=0;
    results_t results;
    char sql[256], sqltxn[80], config[80], likestr[80];
    cdb2_hndl_tp *hndl;
    cdb2_effects_tp effects;
    int isfirst;
    struct timespec start, end, elapsed;
    unsigned long long nsecs, totrecs, average_nsec, totcount=0;

    cfg->numthreads=1;

    snprintf(config, sizeof(config), "%s.cfg", cfg->dbname);
    cdb2_set_comdb2db_config(config);

    if((rc=cdb2_open(&hndl, cfg->dbname, cfg->cltype, 1))!=0)
    {
        fprintf(stderr, "Error allocating handle, %s\n", cdb2_errstr(hndl));
        exit(1);
    }

    /* Set sql */
    snprintf(sql, sizeof(sql), "SELECT count(*), avg(b) FROM tix WHERE c LIKE ?");

    cdb2_bind_index(hndl, 1, CDB2_CSTRING, &likestr, sizeof(likestr));

    /* Start timer */
    clock_gettime(CLOCK_REALTIME, &start);

    if(cfg->dotxn)
    {
        snprintf(sqltxn, sizeof(sqltxn), "BEGIN");

        if((rc=cdb2_run_statement(hndl, sqltxn))!=0)
        {
            fprintf(stderr, "error running begin,  '%s'\n", cdb2_errstr(hndl));
            exit(1);
        }
        do
        {
            rc = cdb2_next_record(hndl);
        }
        while(rc == CDB2_OK);

        if(rc != CDB2_OK_DONE)
        {
            fprintf(stderr, "Error running begin, %s\n", cdb2_errstr(hndl));
            exit(1);
        }
    }

    for(i=0;i<10;i++)
    {
        snprintf(likestr, sizeof(likestr), "%s%%", number_name(i));

        /* Execute sql */
        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "Select error rc=%d, %s\n", rc, 
                    cdb2_errstr(hndl));
            exit(1);
        }

        countloop++;

        do
        {
            rc2 = cdb2_next_record(hndl);
            if(CDB2_OK == rc2)
            {
                int numcols=2, type=CDB2_INTEGER;
                int *icountp;
                long long *llcountp;

                if(numcols!=2) abort();

                /* Add the count to the total number of records found */
                numcols=cdb2_numcolumns(hndl);
                type=cdb2_column_type(hndl, 0);

                if(type != CDB2_INTEGER) abort();

                llcountp=cdb2_column_value(hndl, 0);
                if(llcountp) totcount+=(*llcountp);
            }
        }
        while(rc2 == CDB2_OK);
    }

    if(cfg->dotxn)
    {
        snprintf(sql, sizeof(sql), "COMMIT");

        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "error running commit,  '%s'\n", cdb2_errstr(hndl));
            exit(1);
        }

        if((rc = cdb2_get_effects(hndl, &effects))!=0)
        {
            fprintf(stderr, "Error calling get_effects, %s\n", cdb2_errstr(hndl));
            exit(1);
        }
    }

    /* Stop timer */
    clock_gettime(CLOCK_REALTIME, &end);

    /* Calculate insert time */
    ts_diff(&elapsed, &start, &end);
    nsecs=elapsed.tv_sec;
    nsecs*=1000000000;
    nsecs+=elapsed.tv_nsec;

    totrecs=totcount;

    /* records per second */
    average_nsec = nsecs / totrecs;

    results.total_recs=totrecs;
    results.total_usec = (nsecs / 1000);
    results.average_usec = (average_nsec / 1000);

    output_test_results(cfg, &results, f);

    cdb2_close(hndl);
    return 0;
}

int bulk_insert_union_test(config_t *cfg, FILE *f)
{
    int rc, i, j, sqlsz, k, maxival=(cfg->numrecs/cfg->rows_in_insert), lastidx=0, now;
    results_t results;
    insert_statement_t *inp;
    insert_val_t *iv;
    char sql[256], *isql, config[80];
    cdb2_hndl_tp *hndl;
    cdb2_effects_tp effects;
    int isfirst;
    struct timespec start, end, elapsed;
    unsigned long long nsecs, totrecs, average_nsec;

    tot_records_added=0;
    cfg->numthreads=1;

    snprintf(config, sizeof(config), "%s.cfg", cfg->dbname);
    cdb2_set_comdb2db_config(config);

    if((rc=cdb2_open(&hndl, cfg->dbname, cfg->cltype, 1))!=0)
    {
        fprintf(stderr, "Error allocating handle, %s\n", cdb2_errstr(hndl));
        exit(1);
    }

    inp=(insert_statement_t *)calloc(maxival, sizeof(insert_statement_t));

    for(j=0;j<maxival;j++)
    {
        int fudgemult=(!cfg->dobind) ? (MAXNUMNAME + 16) : 0;
        char tmp[128];
        size_t curoff=0;
        insert_statement_t *in=&(inp[j]);
        in->count=cfg->rows_in_insert;
        in->sql=(char *)malloc(sqlsz=(128 + ((128 + fudgemult) * cfg->rows_in_insert)));
        snprintf(in->sql, sqlsz, "INSERT INTO tix ");
        isfirst=1;

        for(k=0;k<in->count;k++)
        {
            if(isfirst==0)
                strncatend(in->sql, " UNION ", &curoff, sqlsz);
            if(cfg->dobind)
                strncatend(in->sql, "SELECT ?,?,?", &curoff, sqlsz);
            else
            {
                long long r = getrand(cfg);
                snprintf(tmp,sizeof(tmp), "SELECT %d,%lld,'%s'",
                        lastidx++, r, number_name(r));
                strncatend(in->sql, tmp, &curoff, sqlsz);
            }
            isfirst=0;
        }

        if(cfg->dobind)
        {
            in->ival=(insert_val_t *)calloc(in->count, sizeof(insert_val_t));

            for(k=0;k<in->count;k++)
            {
                insert_val_t *ivp=&in->ival[k];
                ivp->a=lastidx++;
                ivp->r=getrand(cfg);
                ivp->number_name=number_name(ivp->r);
            }
        }
        else
            in->ival=NULL;
    }

    if(cfg->txnlevel)
    {
        snprintf(sql, sizeof(sql), "SET TRANSACTION %s\n", cfg->txnlevel);
        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "Error setting transaction level, %s\n", cdb2_errstr(hndl));
            exit(1);
        }
        do
        {
            rc=cdb2_next_record(hndl);
        }
        while(rc == CDB2_OK);
    }

    /* Bind once */
    if(cfg->dobind)
    {
        iv=(insert_val_t *)calloc(cfg->rows_in_insert, sizeof(insert_val_t));

        for(i=0;i<cfg->rows_in_insert;i++)
        {
            cdb2_bind_index(hndl, (i*3)+1, CDB2_INTEGER, &iv[i].a, sizeof(iv[i].a));
            cdb2_bind_index(hndl, (i*3)+2, CDB2_INTEGER, &iv[i].r, sizeof(iv[i].r));
            iv[i].number_name=(char *)calloc(1, MAXNUMNAME);
            cdb2_bind_index(hndl, (i*3)+3, CDB2_CSTRING, 
                    iv[i].number_name,MAXNUMNAME);
        }
    }

    /* Start timer */
    clock_gettime(CLOCK_REALTIME, &start);

    if(cfg->dotxn)
    {
        snprintf(sql, sizeof(sql), "BEGIN");

        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "error running begin,  '%s'\n", cdb2_errstr(hndl));
            exit(1);
        }
        do
        {
            rc = cdb2_next_record(hndl);
        }
        while(rc == CDB2_OK);

        if(rc != CDB2_OK_DONE)
        {
            fprintf(stderr, "Error running begin, %s\n", cdb2_errstr(hndl));
            exit(1);
        }
    }

    /* Run sql */
    for(i=0;i<maxival;i++)
    {
        insert_statement_t *in=&(inp[i]);

        /* Set bound values */
        for(j=0;cfg->dobind && j<cfg->rows_in_insert;j++)
        {
            iv[j].a=in->ival[j].a;
            iv[j].r=in->ival[j].r;
            strncpy(iv[j].number_name, in->ival[j].number_name, MAXNUMNAME);
        }

        /* Execute sql */
        if((rc=cdb2_run_statement(hndl, in->sql))!=0)
        {
            fprintf(stderr, "Insert error rc=%d, %s\n", rc, 
                    cdb2_errstr(hndl));
            exit(1);
        }

        if(!cfg->dotxn && (rc = cdb2_get_effects(hndl, &effects))!=0)
        {
            fprintf(stderr, "Error calling get_effects, %s\n", cdb2_errstr(hndl));
            exit(1);
        }

        tot_records_added+=in->count;
        if(cfg->verbose && ((now=time(NULL)) - lastpr) >= cfg->printivl)
        {
            lastpr=now;
            fprintf(stderr, "Added %llu records\n", tot_records_added);
        }
    }

    /* Commit */
    if(cfg->dotxn)
    {
        snprintf(sql, sizeof(sql), "COMMIT");

        if((rc=cdb2_run_statement(hndl, sql))!=0)
        {
            fprintf(stderr, "error running commit,  '%s'\n", cdb2_errstr(hndl));
            exit(1);
        }

        if((rc = cdb2_get_effects(hndl, &effects))!=0)
        {
            fprintf(stderr, "Error calling get_effects, %s\n", cdb2_errstr(hndl));
            exit(1);
        }
    }

    /* Stop timer */
    clock_gettime(CLOCK_REALTIME, &end);

    /* Calculate insert time */
    ts_diff(&elapsed, &start, &end);
    nsecs=elapsed.tv_sec;
    nsecs*=1000000000;
    nsecs+=elapsed.tv_nsec;

    /* total records */
    totrecs=cfg->numrecs;

    /* records per second */
    average_nsec = nsecs / totrecs;

    results.total_recs = cfg->numrecs;
    results.total_usec = (nsecs / 1000);
    results.average_usec = (average_nsec / 1000);

    output_test_results(cfg, &results, f);

    for(j=0;cfg->alltests && j<maxival;j++)
    {
        insert_statement_t *in=&(inp[j]);
        free(in->sql);
        if(in->ival) free(in->ival);
    }

    if(cfg->alltests) 
    {
        free(inp);
        cdb2_close(hndl);
    }

    return 0;
}

int insert_big_transaction_test(config_t *cfg, FILE *f)
{
    config_t *test_config;
    fastinit(cfg);
    test_config=insert_big_transaction_config(cfg->dbname, ALLTESTS_AGGRESSIVE);
    test_config->numrecs=test_config->rows_in_insert=cfg->numrecs;
    insert_test(test_config, f);
    free(test_config);
}

int insert_transaction_test(config_t *cfg, FILE *f)
{
    config_t *test_config;
    fastinit(cfg);
    test_config=insert_transaction_config(cfg->dbname, ALLTESTS_AGGRESSIVE);
    insert_test(test_config, f);
    free(test_config);
}

int insert1_test(config_t *cfg, FILE *f)
{
    config_t *test_config;
    fastinit(cfg);
    test_config=insert_config(cfg->dbname, ALLTESTS_AGGRESSIVE);
    insert_test(test_config, f);
    free(test_config);
}

/* Run all of the paulbench tests in paulbench order */
int all_tests(config_t *cfg, FILE *f)
{
    config_t *test_config;
    uint32_t flags = 0;
    
    if (cfg->aggressive) 
        flags |= ALLTESTS_AGGRESSIVE;

    if (cfg->verbose) 
        flags |= ALLTESTS_VERBOSE;

    /* The default options are intended to mimic the actual dbbench test as 
     * closely as possible.  The "aggressive" options are guesses as to ways 
     * to run generally the same test, but get better results under comdb2. */

    /* test_10_insert_parallel */
    if(!cfg->override_mode || cfg->override_mode == 10)
    {
        //printf("Running insert_parallel\n");
        test_config=insert_parallel_config(cfg->dbname, flags);
        test_config->txnlevel = cfg->txnlevel;
        insert_test(test_config, f);
        free(test_config);
    }

    /* test_11_delete */
    if(!cfg->override_mode || cfg->override_mode == 11)
    {
        //printf("Running delete\n");
        test_config=delete_config(cfg->dbname, flags);
        test_config->txnlevel = cfg->txnlevel;
        delete_test(test_config, f);
        free(test_config);
    }

    /* test_12_insert_transaction_parallel */
    if(!cfg->override_mode || cfg->override_mode == 12)
    {
        //printf("Running insert_transaction_parallel\n");
        test_config=insert_transaction_parallel_config(cfg->dbname, flags);
        test_config->txnlevel = cfg->txnlevel;
        insert_test(test_config, f);
        free(test_config);
    }

    /* test_13_insert */
    if(!cfg->override_mode || cfg->override_mode == 13)
    {
        //printf("Running insert\n");
        fastinit(cfg);
        test_config=insert_config(cfg->dbname, flags);
        test_config->txnlevel = cfg->txnlevel;
        insert_test(test_config, f);
        free(test_config);
    }

#if 0
    /* test_14_insert_transaction */
    if(!cfg->override_mode || cfg->override_mode == 14)
    {
        //printf("Running insert_transaction\n");
        fastinit(cfg);
        test_config=insert_transaction_config(cfg->dbname, flags);
        test_config->txnlevel = cfg->txnlevel;
        insert_test(test_config, f);
        free(test_config);
    }
#endif

    /* test_40_update */
    if(!cfg->override_mode || cfg->override_mode == 40)
    {
        //printf("Running update\n");
        test_config=update_config(cfg->dbname, flags);
        test_config->txnlevel = cfg->txnlevel;
        update_test(test_config, f);
        free(test_config);
    }

    /* test_41_update_transaction */
    if(!cfg->override_mode || cfg->override_mode == 41)
    {
        //printf("Running update_transaction\n");
        test_config=update_transaction_config(cfg->dbname, flags);
        test_config->txnlevel = cfg->txnlevel;
        update_test(test_config, f);
        free(test_config);
    }

    /* test_50_select */
    if(!cfg->override_mode || cfg->override_mode == 50)
    {
        //printf("Running select\n");
        test_config=select_config(cfg->dbname, flags);
        test_config->txnlevel = cfg->txnlevel;
        select_test(test_config, f);
        free(test_config);
    }

    /* test_51_select_like */
    if(!cfg->override_mode || cfg->override_mode == 51)
    {
        //printf("Running select_like\n");
        test_config=select_like_config(cfg->dbname, flags);
        test_config->txnlevel = cfg->txnlevel;
        select_like_test(test_config, f);
        free(test_config);
    }

    /* test_60_bulk_insert */
    if(!cfg->override_mode || cfg->override_mode == 60)
    {
        //printf("Running bulk_insert\n");
        fastinit(cfg);
        test_config=bulk_insert_config(cfg->dbname, flags);
        test_config->txnlevel = cfg->txnlevel;
        insert_test(test_config, f);
        free(test_config);
    }

    /* test_61_bulk_insert_transaction */
    if(!cfg->override_mode || cfg->override_mode == 61)
    {
        //printf("Running bulk_insert_transaction\n");
        fastinit(cfg);
        test_config=bulk_insert_transaction_config(cfg->dbname, flags);
        test_config->txnlevel = cfg->txnlevel;
        insert_test(test_config, f);
        free(test_config);
    }

    /* test_62_bulk_insert_union */
    if(!cfg->override_mode || cfg->override_mode == 62)
    {
        //printf("Running bulk_insert_union\n");
        fastinit(cfg);
        test_config=bulk_insert_union_config(cfg->dbname, flags);
        test_config->txnlevel = cfg->txnlevel;
        bulk_insert_union_test(test_config, f);
        free(test_config);
    }

    /* test_63_bulk_insert_transaction_union */
    if(!cfg->override_mode || cfg->override_mode == 63)
    {
        //printf("Running bulk_insert_union_transaction\n");
        fastinit(cfg);
        test_config=bulk_insert_union_transaction_config(cfg->dbname, flags);
        test_config->txnlevel = cfg->txnlevel;
        bulk_insert_union_test(test_config, f);
        free(test_config);
    }

    return 0;
}

int main(int argc, char *argv[])
{
    config_t        *cfg=default_config();
    FILE            *f=stdout;
    int             err=0, opt, omode;

    argv0=argv[0];

    setvbuf(stdout, (char *)NULL, _IOLBF, 0);

    while ((opt = getopt(argc,argv,"d:c:x:T:r:i:l:m:f:buDBEUISLAGstX:zqQ:h"))!=EOF)
    {
        switch(opt)
        {
            case 'd':
                cfg->dbname=optarg;
                break;

            case 'f':
                if(!(f=fopen(optarg, "w")))
                {
                    fprintf(stderr, "Error opening '%s', %s\n", optarg, 
                            strerror(errno));
                    err++;
                }
                break;

            case 'I':
                cfg->mode=MODE_INSERT;
                break;

            case 'E':
                cfg->mode=MODE_BULK_INSERT;
                cfg->verbose=0;
                cfg->numthreads=1;
                cfg->numrecs=100000;
                cfg->rows_in_insert=100000;
                cfg->dobind=0;
                cfg->alltests=1;
                break;

            case 'B':
                cfg->mode=MODE_BIUNION;
                if(!cfg->setnumrecs)
                {
                    cfg->numrecs=100000;
                }
                if(!cfg->setrowsininsert)
                {
                    cfg->rows_in_insert=50;
                }
                break;

            case 'D':
                cfg->mode=MODE_DELETE;
                if(!cfg->setnumrecs)
                {
                    cfg->numrecs=5000;
                }
                break;

            case 'U':
                cfg->mode=MODE_UPDATE;
                if(!cfg->setnumrecs)
                {
                    cfg->numrecs=100;
                }
                break;

            case 'S':
                cfg->mode=MODE_SELECT;
                break;

            case 'L':
                cfg->mode=MODE_SELECT_LIKE;
                break;

            case 'A':
                cfg->mode=MODE_ALLTESTS;
                cfg->aggressive=0;
                cfg->verbose=0;
                break;

            case 'G':
                cfg->mode=MODE_ALLTESTS;
                cfg->aggressive=1;
                cfg->verbose=0;
                break;

            case 's':
                cfg->mode=MODE_1RECINS;
                cfg->aggressive=1;
                cfg->verbose=0;
                break;

            case 't':
                cfg->mode=MODE_INSTXN;
                cfg->aggressive=1;
                cfg->verbose=0;
                break;

            case 'X':
                cfg->mode=MODE_BIGTXN;
                cfg->aggressive=1;
                cfg->numrecs=cfg->rows_in_insert=atoi(optarg);
                cfg->verbose=0;
                break;

            case 'b':
                cfg->dobind=1;
                break;

            case 'u':
                cfg->dobind=0;
                break;

            case 'm':
                cfg->select_max=cfg->updmax=atoi(optarg);
                break;

            case 'l':
                cfg->loadcnt=atoi(optarg);
                break;

            case 'c':
                cfg->cltype=optarg;
                break;

            case 'x':
                cfg->txnlevel=optarg;
                break;

            case 'T':
                cfg->numthreads=atoi(optarg);
                break;

            case 'r':
                cfg->select_interval=cfg->numrecs=atoi(optarg);
                cfg->setnumrecs=1;
                break;

            case 'v':
                cfg->verbose=1;
                break;

            case 'Q':
                cfg->mode=MODE_ALLTESTS;
                cfg->aggressive=1;
                cfg->verbose=0;
                omode=atoi(optarg);
                switch(omode)
                {
                    case 10:
                    case 11:
                    case 12:
                    case 13:
                    case 14:
                    case 40:
                    case 41:
                    case 50:
                    case 51:
                    case 60:
                    case 61:
                    case 62:
                    case 63:
                        cfg->override_mode=atoi(optarg);
                        break;
                    default:
                        fprintf(stderr, "Invalid override mode.  Valid modes are:\n");
                        fprintf(stderr, "   10 - insert parallel\n");
                        fprintf(stderr, "   11 - delete\n");
                        fprintf(stderr, "   12 - insert transaction parallel\n");
                        fprintf(stderr, "   13 - insert\n");
                        fprintf(stderr, "   14 - insert transaction\n");
                        fprintf(stderr, "   40 - update\n");
                        fprintf(stderr, "   41 - update transaction\n");
                        fprintf(stderr, "   50 - select\n");
                        fprintf(stderr, "   51 - select-like\n");
                        fprintf(stderr, "   60 - bulk-insert\n");
                        fprintf(stderr, "   61 - bulk-insert-transaction\n");
                        fprintf(stderr, "   62 - bulk-insert-union\n");
                        fprintf(stderr, "   63 - bulk-insert-transaction-union\n");
                        err++;
                        break;
                }

            case 'q':
                cfg->verbose=0;
                break;

            case 'i':
                cfg->rows_in_insert=atoi(optarg);
                cfg->setrowsininsert=1;
                break;

            case 'z':
                cfg->dotxn=1;
                break;

            case 'h':
                usage(stdout);
                break;

            default:
                fprintf(stderr, "Unknown option, '%c'.\n", opt);
                err++;
                break;
        }
    }

    if(NULL == cfg->dbname)
    {
        fprintf(stderr, "Dbname is not set!\n");
        err++;
    }

    if(err)
    {
        usage(stderr);
    }

    srand(1);
    signal(SIGPIPE, SIG_IGN);
    lastpr=time(NULL);

    switch(cfg->mode)
    {
        case MODE_INSERT:
            insert_test(cfg, f);
            break;

        case MODE_DELETE:
            delete_test(cfg, f);
            break;

        case MODE_UPDATE:
            update_test(cfg, f);
            break;

        case MODE_SELECT:
            select_test(cfg, f);
            break;

        case MODE_SELECT_LIKE:
            select_like_test(cfg, f);
            break;

        case MODE_BULK_INSERT:
            insert_test(cfg, f);
            break;

        case MODE_BIUNION:
            bulk_insert_union_test(cfg, f);
            break;

        case MODE_ALLTESTS:
            all_tests(cfg, f);
            break;

        case MODE_1RECINS:
            insert1_test(cfg, f);
            break;

        case MODE_INSTXN:
            insert_transaction_test(cfg, f);
            break;

        case MODE_BIGTXN:
            insert_big_transaction_test(cfg, f);
            break;

        default:
            fprintf(stderr, "Invalid test mode\n");
            usage(stderr);
            break;

    }
    return 0;
}

