/* PgDBF - Quickly convert DBF files to PostgreSQL                       */
/* Copyright (C) 2008-2012  Kirk Strauser <kirk@strauser.com>            */
/*                                                                       */
/* This program is free software: you can redistribute it and/or modify  */
/* it under the terms of the GNU General Public License as published by  */
/* the Free Software Foundation, either version 3 of the License, or     */
/* (at your option) any later version.                                   */
/*                                                                       */
/* This program is distributed in the hope that it will be useful,       */
/* but WITHOUT ANY WARRANTY; without even the implied warranty of        */
/* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         */
/* GNU General Public License for more details.                          */
/*                                                                       */
/* You should have received a copy of the GNU General Public License     */
/* along with this program.  If not, see <http://www.gnu.org/licenses/>. */

/* This sets glibc (and possibly Solaris's libc?) to replace fopen, fstat,
 * etc. with their 64-bit equivalents so that they can open files larger
 * than 2GB. FreeBSD and OS X handle large files by default. */
#define _FILE_OFFSET_BITS 64

#include <config.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(HAVE_ICONV)
#include <iconv.h>
#endif

/* This should be big enough to hold most of the varchars and memo fields
 * that you'll be processing.  If a given piece of data won't fit in a
 * buffer of this size, then a temporary buffer will be allocated for it. */
#define STATICBUFFERSIZE 1024 * 1024 * 4

/* Attempt to read approximately this many bytes from the .dbf file at once.
 * The actual number may be adjusted up or down as appropriate. */
#define DBFBATCHTARGET 1024 * 1024 * 16

/* Old versions of FoxPro (and probably other programs) store the memo file
 * record number in human-readable ASCII. Newer versions of FoxPro store it
 * as a 32-bit packed int. */
#define NUMERICMEMOSTYLE 0
#define PACKEDMEMOSTYLE 1

/* Don't edit this! It's defined in the XBase specification. */
#define XBASEFIELDNAMESIZE 11

/* This is the maximum size a generated PostgreSQL column size can possibly
 * be. It's used when making unique versions of duplicated field names.
 *
 *    11 bytes for the maximum XBase field name length
 *    1 byte for a "_" separator
 *    5 bytes for the numeric "serial number" portion
 *    1 byte for the trailing \0
 */
#define MAXCOLUMNNAMESIZE (XBASEFIELDNAMESIZE + 7)

static char staticbuf[STATICBUFFERSIZE + 1];

/* The list of reserved words that can't be used as column names, as per
 * http://www.postgresql.org/docs/x.y/static/sql-keywords-appendix.html ,
 * for (x.y) in 8.0, 8.1, 8.2, 8.3, 8.4, 9.0. This list includes words
 * longer than XBase's 11-characted column names for completeness, even if
 * they'll never be matched. */
static const char *RESERVEDWORDS[] = {
    "all",                 /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "analyse",             /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "analyze",             /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "and",                 /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "any",                 /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "array",               /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "as",                  /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "asc",                 /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "asymmetric",          /* PostgreSQL versions 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "both",                /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "case",                /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "cast",                /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "check",               /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "collate",             /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "column",              /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "constraint",          /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "create",              /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "current_catalog",     /* PostgreSQL versions 8.4, 9.0, 9.1 */
    "current_date",        /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "current_role",        /* PostgreSQL versions 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "current_time",        /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "current_timestamp",   /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "current_user",        /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "default",             /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "deferrable",          /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "desc",                /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "distinct",            /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "do",                  /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "else",                /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "end",                 /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "except",              /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "false",               /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "fetch",               /* PostgreSQL versions 8.4, 9.0, 9.1 */
    "for",                 /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "foreign",             /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "from",                /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "grant",               /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "group",               /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "having",              /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "in",                  /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "initially",           /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "intersect",           /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "into",                /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "leading",             /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "limit",               /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "localtime",           /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "localtimestamp",      /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "new",                 /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4 */
    "not",                 /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "null",                /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "off",                 /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4 */
    "offset",              /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "old",                 /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4 */
    "on",                  /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "only",                /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "or",                  /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "order",               /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "placing",             /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "primary",             /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "references",          /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "returning",           /* PostgreSQL versions 8.2, 8.3, 8.4, 9.0, 9.1 */
    "select",              /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "session_user",        /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "some",                /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "symmetric",           /* PostgreSQL versions 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "table",               /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "then",                /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "to",                  /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "trailing",            /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "true",                /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "union",               /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "unique",              /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "user",                /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "using",               /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "variadic",            /* PostgreSQL versions 8.4, 9.0, 9.1 */
    "when",                /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "where",               /* PostgreSQL versions 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1 */
    "window",              /* PostgreSQL versions 8.4, 9.0, 9.1 */
    "with",                /* PostgreSQL versions 8.3, 8.4, 9.0, 9.1 */
    NULL,
};

typedef struct {
    int8_t   signature;
    int8_t   year;
    int8_t   month;
    int8_t   day;
    uint32_t recordcount;
    uint16_t headerlength;
    uint16_t recordlength;
    int8_t   reserved1[2];
    int8_t   incomplete;
    int8_t   encrypted;
    int8_t   reserved2[4];      /* Free record thread */
    int8_t   reserved3[8];      /* Reserved for multi-user dBASE */
    int8_t   mdx;
    int8_t   language;
    int8_t   reserved4[2];
} DBFHEADER;

typedef struct {
    char    name[XBASEFIELDNAMESIZE];
    char    type;
    int32_t memaddress;
    uint8_t length;
    uint8_t decimals;
    int16_t flags;              /* Reserved for multi-user dBase */
    char    workareaid;
    char    reserved1[2];       /* Reserved for multi-user dBase */
    char    setfields;
    char    reserved2[7];
    char    indexfield;
} DBFFIELD;

typedef struct {
    char nextblock[4];
    char reserved1[2];
    char blocksize[2];
    char reserved2[504];
} MEMOHEADER;

typedef struct {
    char *formatstring;
    int   memonumbering;
} PGFIELD;

#if defined(HAVE_ICONV)
static iconv_t conv_desc = NULL;

static char* convertcharset(const char* inputstring, size_t* inputsize)
{
    char   *inbuf;
    char   *outbuf;
    size_t  inbytesleft;
    size_t  outbyteslen;
    size_t  outbytesleft;

    inbuf = (char *)inputstring;
    inbytesleft = *inputsize;
    outbyteslen = inbytesleft * 4 + 1;
    outbytesleft = outbyteslen;
    outbuf = calloc(outbyteslen, 1);

    char *outbufstart = outbuf;

    size_t iconv_value = iconv(conv_desc, &inbuf, &inbytesleft, &outbuf, &outbytesleft);

    /* Handle failures. */
    if(iconv_value == (size_t)-1) {
        fprintf(stderr, "iconv failed\n");
        switch(errno) {
            case EILSEQ:
                fprintf(stderr, "Invalid multibyte sequence.\n");
                break;
            case EINVAL:
                fprintf(stderr, "Incomplete multibyte sequence.\n");
                break;
            case E2BIG:
                fprintf(stderr, "No more room (increase size of outbuf in pgdbf.h).\n");
                break;
            default:
                fprintf(stderr, "Error: %s.\n", strerror(errno));
        }
        exit(1);
    }

    *inputsize = outbyteslen - outbytesleft;

    return outbufstart;
}
#endif

static void exitwitherror(const char *message, const int systemerror) {
    /* Print the given error message to stderr, then exit.  If systemerror
     * is true, then use perror to explain the value in errno. */
    if(systemerror) {
        perror(message);
    } else {
        fprintf(stderr, "%s\n", message);
    }
    exit(EXIT_FAILURE);
}

static void safeprintbuf(const char *buf, const size_t inputsize) {
    /* Print a string, insuring that it's fit for use in a tab-delimited
     * text file */
    char       *convbuf;
    char       *targetbuf;
    const char *s;
    const char *lastchar;
    char       *t;
    size_t     realsize = 0;

    /* Shortcut for empty strings */
    if(*buf == '\0') {
        return;
    }

    /* Find the rightmost non-space, non-null character */
    for(s = buf + inputsize - 1; s >= buf; s--) {
        if(*s != ' ' && *s != '\0') {
            break;
        }
    }

    /* If there aren't any non-space characters, skip the output part */
    if(s < buf) {
        return;
    }

    lastchar = s;
    realsize = s - buf + 1;
    convbuf = (char *)buf;

#if defined(HAVE_ICONV)
    if(conv_desc != NULL) {
        convbuf = convertcharset(buf, &realsize);
        lastchar = convbuf + realsize - 1;
    }
#endif

    if(realsize * 2 < STATICBUFFERSIZE) {
        targetbuf = staticbuf;
    } else {
        targetbuf = malloc(realsize * 2 + 1);
        if(targetbuf == NULL) {
            exitwitherror("Unable to malloc the escape output buffer", 1);
        }
    }

    /* Re-write invalid characters to their SQL-safe alternatives */
    t = targetbuf;
    for(s = convbuf; s <= lastchar; s++) {
        switch(*s) {
        case '\\':
            *t++ = '\\';
            *t++ = '\\';
            break;
        case '\n':
            *t++ = '\\';
            *t++ = 'n';
            break;
        case '\r':
            *t++ = '\\';
            *t++ = 'r';
            break;
        case '\t':
            *t++ = '\\';
            *t++ = 't';
            break;
        default:
            *t++ = *s;
        }
    }
    *t = '\0';

    printf("%s", targetbuf);

#if defined(HAVE_ICONV)
    if(conv_desc != NULL) {
        free(convbuf);
    }
#endif

    if(targetbuf != staticbuf) {
        free(targetbuf);
    }
}

int progressdots = 1;

void updateprogressbar(int percent) {
    int newprogressdots = percent / 2;
    for(; progressdots <= newprogressdots; progressdots++) {
        putc('.', stderr);
        if(progressdots && !(progressdots % 5)){
            fprintf(stderr, "%d", progressdots * 2);
        }
    }
    if(percent == 100) {
        fprintf(stderr, "\n");
    }
    fflush(stderr);
}

/* Endian-specific code.  Define functions to convert input data to the
 * required form depending on the endianness of the host architecture. */

#define SWAP8BYTES(rightend, wrongendcharptr)   \
    const char *src = wrongendcharptr + 7;      \
    memcpy((char *) &rightend    , src--, 1);   \
    memcpy((char *) &rightend + 1, src--, 1);   \
    memcpy((char *) &rightend + 2, src--, 1);   \
    memcpy((char *) &rightend + 3, src--, 1);   \
    memcpy((char *) &rightend + 4, src--, 1);   \
    memcpy((char *) &rightend + 5, src--, 1);   \
    memcpy((char *) &rightend + 6, src--, 1);   \
    memcpy((char *) &rightend + 7, src  , 1);

#define SWAPANDRETURN8BYTES(wrongendcharptr)   \
    int64_t rightend;                          \
    SWAP8BYTES(rightend, wrongendcharptr)      \
    return rightend;

#define SWAPANDRETURN4BYTES(wrongendcharptr)   \
    const char *src = wrongendcharptr + 3;     \
    int32_t rightend;                          \
    memcpy((char*) &rightend    , src--, 1);   \
    memcpy((char*) &rightend + 1, src--, 1);   \
    memcpy((char*) &rightend + 2, src--, 1);   \
    memcpy((char*) &rightend + 3, src  , 1);   \
    return rightend;

#define SWAPANDRETURN2BYTES(wrongendcharptr)   \
    const char *src = wrongendcharptr + 1;     \
    int16_t rightend;                          \
    memcpy((char*) &rightend    , src--, 1);   \
    memcpy((char*) &rightend + 1, src  , 1);   \
    return rightend;

/* Integer-to-integer */

static int64_t nativeint64_t(const int64_t rightend) {
    /* Leave a 64-bit integer alone */
    return rightend;
}

static int64_t swappedint64_t(const int64_t wrongend) {
    /* Change the endianness of a 64-bit integer */
    SWAPANDRETURN8BYTES(((char *) &wrongend))
}

static int32_t nativeint32_t(const int32_t rightend) {
    /* Leave a 32-bit integer alone */
    return rightend;
}

static int32_t swappedint32_t(const int32_t wrongend) {
    /* Change the endianness of a 32-bit integer */
    SWAPANDRETURN4BYTES(((char*) &wrongend))
}

static int16_t nativeint16_t(const int16_t rightend) {
    /* Leave a 16-bit integer alone */
    return rightend;
}

static int16_t swappedint16_t(const int16_t wrongend) {
    /* Change the endianness of a 16-bit integer */
    SWAPANDRETURN2BYTES(((char*) &wrongend))
}

/* String-to-integer */

static int64_t snativeint64_t(const char *buf) {
    /* Interpret the first 8 bytes of buf as a 64-bit int */
    int64_t output;
    memcpy(&output, buf, 8);
    return output;
}

static int64_t sswappedint64_t(const char *buf) {
    /* The byte-swapped version of snativeint64_t */
    SWAPANDRETURN8BYTES(buf)
}

static int32_t snativeint32_t(const char *buf) {
    /* Interpret the first 4 bytes of buf as a 32-bit int */
    int32_t output;
    memcpy(&output, buf, 4);
    return output;
}

static int32_t sswappedint32_t(const char *buf) {
    /* The byte-swapped version of snativeint32_t */
    SWAPANDRETURN4BYTES(buf)
}

static int16_t snativeint16_t(const char *buf) {
    /* Interpret the first 2 bytes of buf as a 16-bit int */
    int16_t output;
    memcpy(&output, buf, 2);
    return output;
}

static int16_t sswappedint16_t(const char *buf) {
    /* The byte-swapped version of snativeint16_t */
    SWAPANDRETURN2BYTES(buf)
}

#ifdef WORDS_BIGENDIAN
#define bigint64_t     nativeint64_t
#define littleint64_t  swappedint64_t

#define bigint32_t     nativeint32_t
#define littleint32_t  swappedint32_t

#define bigint16_t     nativeint16_t
#define littleint16_t  swappedint16_t

#define sbigint64_t    snativeint64_t
#define slittleint64_t sswappedint64_t

#define sbigint32_t    snativeint32_t
#define slittleint32_t sswappedint32_t

#define sbigint16_t    snativeint16_t
#define slittleint16_t sswappedint16_t

static double sdouble(const char *buf) {
    /* Doubles are stored as 64-bit little-endian, so swap ends */
    union {
        int64_t asint64;
        double  asdouble;
    } inttodouble;

    SWAP8BYTES(inttodouble.asint64, buf)
    return inttodouble.asdouble;
}
#else
#define bigint64_t     swappedint64_t
#define littleint64_t  nativeint64_t

#define bigint32_t     swappedint32_t
#define littleint32_t  nativeint32_t

#define bigint16_t     swappedint16_t
#define littleint16_t  nativeint16_t

#define sbigint64_t    sswappedint64_t
#define slittleint64_t snativeint64_t

#define sbigint32_t    sswappedint32_t
#define slittleint32_t snativeint32_t

#define sbigint16_t    sswappedint16_t
#define slittleint16_t snativeint16_t

static double sdouble(const char *buf) {
    /* Interpret the first 8 bytes of buf as a double */
    double output;
    memcpy(&output, buf, 8);
    return output;
}

#endif
