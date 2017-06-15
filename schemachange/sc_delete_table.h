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

#ifndef INCLUDE_SC_FASTINIT_H
#define INCLUDE_SC_FASTINIT_H

int delete_table(char *table);
int do_fastinit_int(struct schema_change_type *s, struct ireq *iniq);
int finalize_fastinit_table(struct schema_change_type *s);
int finalize_fastinit_table_prepare(struct schema_change_type *s, void *transac,
                                    int *bdberr);
int finalize_fastinit_table_tran(struct schema_change_type *s, void *tran,
                                 int olddb_bthashsz, int *bdberr);
int finalize_fastinit_table_drop(struct schema_change_type *s, void *transac,
                                 int *bdberr);
void finalize_fastinit_table_drop_done(struct schema_change_type *s);
int finalize_fastinit_table_backout(struct schema_change_type *s, int *bdberr);
#endif
