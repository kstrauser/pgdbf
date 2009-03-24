/* PgDBF - Quickly convert DBF files to PostgreSQL                       */
/* Copyright (C) 2008,2009  Kirk Strauser <kirk@daycos.com>              */
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

#include <config.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* This should be big enough to hold most of the varchars and memo fields
 * that you'll be processing.  If a given piece of data won't fit in a
 * buffer of this size, then a temporary buffer will be allocated for it. */
#define STATICBUFFERSIZE 1024 * 1024

static char staticbuf[STATICBUFFERSIZE + 1];

/* The list of reserved words that can't be used as column names, as per
 * http://www.postgresql.org/docs/8.3/static/sql-keywords-appendix.html .
 * This list includes words longer than XBase's 11-characted column names
 * for completeness. */
static const char *RESERVEDWORDS[] = {
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "column",
    "constraint",
    "create",
    "current_date",
    "current_role",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "for",
    "foreign",
    "from",
    "grant",
    "group",
    "having",
    "in",
    "initially",
    "intersect",
    "into",
    "leading",
    "limit",
    "localtime",
    "localtimestamp",
    "new",
    "not",
    "null",
    "off",
    "offset",
    "old",
    "on",
    "only",
    "or",
    "order",
    "placing",
    "primary",
    "references",
    "returning",
    "select",
    "session_user",
    "some",
    "symmetric",
    "table",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "when",
    "where",
    "with",
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
    int8_t   reserved2[4];	/* Free record thread */
    int8_t   reserved3[8];	/* Reserved for multi-user dBASE */
    int8_t   mdx;
    int8_t   language;
    int8_t   reserved4[2];
} DBFHEADER;

typedef struct 
{
    char    name[11];
    char    type;
    int32_t memaddress;
    uint8_t length;
    uint8_t decimals;
    int16_t flags;		/* Reserved for multi-user dBase */
    char    workareaid;
    char    reserved1[2];	/* Reserved for multi-user dBase */
    char    setfields;
    char    reserved2[7];
    char    indexfield;
} DBFFIELD;

typedef struct 
{
    char nextblock[4];
    char reserved1[2];
    char blocksize[2];
    char reserved2[504];
} MEMOHEADER;

static void exitwitherror(const char *message, const int systemerror)
{
    /* Print the given error message to stderr, then exit.  If systemerror
     * is true, then use perror to explain the value in errno. */
    if(systemerror) {
        perror(message);
    } else {
        fprintf(stderr, "%s\n", message);
    }
    exit(EXIT_FAILURE);
}

static void safeprintbuf(const char *buf, const size_t inputsize)
{
    /* Print a string, insuring that it's fit for use in a tab-delimited
     * text file */
    char       *targetbuf;
    const char *s;
    const char *lastchar;
    char       *t;
    int         realsize = 0;

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
    for(s = buf; s <= lastchar; s++) {
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

    if(targetbuf != staticbuf) {
	free(targetbuf);
    }
}

/* Endian-specific code.  Define functions to convert input data to the
 * required form depending on the endianness of the host architecture. */

#ifdef WORDS_BIGENDIAN
static int64_t littleint64_t(const int64_t wrongend)
{
    /* Change the endianness of a 64-bit integer */
    return (int64_t) (((wrongend & 0xff00000000000000LL) >> 56) |
		      ((wrongend & 0x00ff000000000000LL) >> 40) |
		      ((wrongend & 0x0000ff0000000000LL) >> 24) |
		      ((wrongend & 0x000000ff00000000LL) >> 8)  |
		      ((wrongend & 0x00000000ff000000LL) << 8)  |
		      ((wrongend & 0x0000000000ff0000LL) << 24) |
		      ((wrongend & 0x000000000000ff00LL) << 40) |
		      ((wrongend & 0x00000000000000ffLL) << 56));
}

static int32_t littleint32_t(const int32_t wrongend)
{
    /* Change the endianness of a 32-bit integer */
    return (int32_t) (((wrongend & 0xff000000) >> 24) |
		      ((wrongend & 0x00ff0000) >> 8)  |
		      ((wrongend & 0x0000ff00) << 8)  |
		      ((wrongend & 0x000000ff) << 24));
}

static int16_t littleint16_t(const int16_t wrongend)
{
    /* Change the endianness of a 16-bit integer */
    return (int16_t) (((wrongend & 0xff00) >> 8) |
		      ((wrongend & 0x00ff) << 8));
}

static int32_t sbigint32_t(const char *buf) 
{
    /* Interpret the first 4 bytes of buf as a 32-bit int */
    int32_t output;
    memcpy(&output, buf, 4);
    return output;
}

static int16_t sbigint16_t(const char *buf) 
{
    /* Interpret the first 2 bytes of buf as a 16-bit int */
    int16_t output;
    memcpy(&output, buf, 2);
    return output;
}

static int64_t slittleint64_t(const char *buf)
{
    /* The byte-swapped version of sbigint64_t */
    int64_t output;
    memcpy(&output, buf, 8);
    return littleint64_t(output);
}

static int32_t slittleint32_t(const char *buf)
{
    /* The byte-swapped version of sbigint32_t */
    int32_t output;
    memcpy(&output, buf, 4);
    return littleint32_t(output);
}

static double sdouble(const char *buf)
{
    /* The byte-swapped version of snativedouble */
    union 
    {
	int64_t asint64;
	double  asdouble;
    } inttodouble;

    inttodouble.asint64 = slittleint64_t(buf);
    return inttodouble.asdouble;
}
#else
static int32_t littleint32_t(const int32_t rightend)
{
    /* Leave a 32-bit integer alone */
    return rightend;
}

static int16_t littleint16_t(const int16_t rightend)
{
    /* Leave a 16-bit integer alone */
    return rightend;
}

static int32_t bigint32_t(const int32_t wrongend)
{
    /* Change the endianness of a 32-bit integer */
    return (int32_t) (((wrongend & 0xff000000) >> 24) |
		      ((wrongend & 0x00ff0000) >> 8)  |
		      ((wrongend & 0x0000ff00) << 8)  |
		      ((wrongend & 0x000000ff) << 24));
}

static int16_t bigint16_t(const int16_t wrongend)
{
    /* Change the endianness of a 16-bit integer */
    return (int16_t) (((wrongend & 0xff00) >> 8) |
		      ((wrongend & 0x00ff) << 8));
}

static int64_t slittleint64_t(const char *buf) 
{
    /* Interpret the first 8 bytes of buf as a 64-bit int */
    int64_t output;
    memcpy(&output, buf, 8);
    return output;
}

static int32_t slittleint32_t(const char *buf) 
{
    /* Interpret the first 4 bytes of buf as a 32-bit int */
    int32_t output;
    memcpy(&output, buf, 4);
    return output;
}

static double sdouble(const char *buf)
{
    /* Interpret the first 8 bytes of buf as a double */
    double output;
    memcpy(&output, buf, 8);
    return output;
}

static int32_t sbigint32_t(const char *buf)
{
    /* The byte-swapped version of slittleint32_t */
    int32_t output;
    memcpy(&output, buf, 4);
    return bigint32_t(output);
}

static int16_t sbigint16_t(const char *buf) 
{
    /* The byte-swapped version of slittleint16_t */
    int16_t output;
    memcpy(&output, buf, 2);
    return bigint16_t(output);
}
#endif
