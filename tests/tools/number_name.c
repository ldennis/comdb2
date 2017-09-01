#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define MAXLEN 4096

typedef struct numbers
{
    int     div;
    char    name[20];
} 
numbers_t;

numbers_t nums[] = {
    { 15, "quadrillion" },
    { 12, "trillion" },
    {  9, "billion" },
    {  6, "million" },
    {  3, "thousand" }
};

numbers_t ones[] = {
    {  0, "zero" },
    {  0, "one" },
    {  0, "two" },
    {  0, "three" },
    {  0, "four" },
    {  0, "five" },
    {  0, "six" },
    {  0, "seven" },
    {  0, "eight" },
    {  0, "nine" },
    {  0, "ten" },
    {  0, "eleven" },
    {  0, "twelve" },
    {  0, "thirteen" },
    {  0, "fourteen" },
    {  0, "fifteen" },
    {  0, "sixteen" },
    {  0, "seventeen" },
    {  0, "eighteen" },
    {  0, "nineteen" }
};

numbers_t tens[] = {
    {  1, "-" },
    {  1, "ten" },
    {  1, "twenty" },
    {  1, "thirty" },
    {  1, "forty" },
    {  1, "fifty" },
    {  1, "sixty" },
    {  1, "seventy" },
    {  1, "eighty" },
    {  1, "ninety" },
};

static long long power(int base, int pow)
{
    long long rtn=1;
    int i;

    for(i=0;i<pow;i++)
    {
        rtn*=base;
    }

    return rtn;
}

char *number_name(long long n)
{
    char txt[MAXLEN] = {0};
    long long div;
    int i;

    if(n<0)
    {
        strncat(txt,"negative ", MAXLEN);
        n=0-n;
    }

    for(i=0;i<sizeof(nums)/sizeof(numbers_t);i++)
    {
        div=power(10,nums[i].div);
        if (n>=div)
        {
            char *x=number_name(n/div);
            strncat(txt, x, MAXLEN);
            free(x);
            strncat(txt, " ", MAXLEN);
            strncat(txt, nums[i].name, MAXLEN);
            strncat(txt, " ", MAXLEN);
            n%=div;
        }
    }

    if (n>=100)
    {
        strncat(txt, ones[n/100].name, MAXLEN);
        strncat(txt, " hundred ", MAXLEN);
        n%=100;
    }

    if (n>=20)
    {
        strncat(txt, tens[n/10].name, MAXLEN);
        strncat(txt, " ", MAXLEN);
        n%=10;
    }

    if (n>0)
    {
        strncat(txt, ones[n].name, MAXLEN);
    }

    while(txt[strlen(txt) - 1] == ' ')
    {
        txt[strlen(txt) - 1] = '\0';
    }

    if(0 == strlen(txt))
    {
        snprintf(txt, MAXLEN, "zero");
    }

    return strdup(txt);
}
