#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include "ts_diff.h"


void ts_diff(struct timespec *elapsed, struct timespec *start, struct timespec *end)
{
    if(end->tv_sec < start->tv_sec)
    {
        fprintf(stderr, "%s invalid input, end sec = %d, start sec = %d\n", __func__, end->tv_sec, start->tv_sec);
        exit(1);
    }
    elapsed->tv_sec=end->tv_sec - start->tv_sec;

    if(end->tv_nsec < start->tv_nsec)
    {
        elapsed->tv_sec--;
        elapsed->tv_nsec=(end->tv_nsec + 1000000000) - start->tv_nsec;
        if(elapsed->tv_nsec > 1000000000)
        {
            fprintf(stderr, "Invalid entry for tv_nsec, %d\n", elapsed->tv_nsec);
            exit(1);
        }
    }
    else
    {
        elapsed->tv_nsec=end->tv_nsec - start->tv_nsec;
    }
}

