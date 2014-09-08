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

#include <config.h>
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "pgdbf.h"

#define STANDARDOPTS "cCdDeEhm:nNpPqQtTuU"

int main(int argc, char **argv) {
    /* Describing the DBF file */
    char          *dbffilename;
    FILE          *dbffile;
    DBFHEADER      dbfheader;
    DBFFIELD      *fields;
    PGFIELD       *pgfields;
    size_t         dbffieldsize;
    size_t         fieldcount;     /* Number of fields for this DBF file */
    unsigned int   recordbase;     /* The first record in a batch of records */
    unsigned int   dbfbatchsize;   /* How many DBF records to read at once */
    unsigned int   batchindex;     /* The offset inside the current batch of
                                    * DBF records */
    int            skipbytes;      /* The length of the Visual FoxPro DBC in
                                    * this file (if there is one) */
    int            fieldarraysize; /* The length of the field descriptor
                                    * array */
    int            fieldnum;       /* The current field beind processed */
    uint8_t        terminator;     /* Testing for terminator bytes */

    /* Describing the memo file */
    MEMOHEADER  *memoheader;
    char        *memofilename = NULL;
    int          memofd;
    struct stat  memostat;
    int32_t      memoblocknumber;
    int          memofileisdbase3 = 0;

    void        *memomap = NULL;     /* Pointer to the mmap of the memo file */
    char        *memorecord;         /* Pointer to the current memo block */
    size_t       memoblocksize = 0;  /* The length of each memo block */
    size_t       memofilesize;
    size_t       memorecordoffset;

    /* Processing and misc */
    char *inputbuffer;
    char *outputbuffer;
    char *bufoffset;
    char *s;
    char *t;
    char *u;
    int  lastcharwasreplaced = 0;
    int     i;
    int     j;
    int     isreservedname;
    int     printed;
    size_t  blocksread;
    size_t  longestfield = 32;  /* Make sure we leave at least enough room
                                 * to print out long formatted numbers, like
                                 * currencies. */

    /* Datetime calculation stuff */
    int32_t juliandays;
    int32_t seconds;
    int     hours;
    int     minutes;

    /* Command line option parsing */
    int     opt;
    int     optexitcode = -1;   /* Left at -1 means that the arguments were
                                 * valid and the program should run.
                                 * Anything else is an exit code and the
                                 * program will stop. */
    char    optvalidargs[sizeof(STANDARDOPTS) + 3];

    /* Default values for command line options */
    int     optnumericasnumeric = 1;
    int     optshowprogress = 0;
    int     optusecreatetable = 1;
    int     optusedroptable = 1;
    int     optuseifexists = 1;
    int     optusequotedtablename = 0;
    int     optusetransaction = 1;
    int     optusetruncatetable = 0;

    /* Describing the PostgreSQL table */
    char *tablename;
    char *baretablename;
    char (*fieldnames)[MAXCOLUMNNAMESIZE];
    int isuniquename;
    char basename[MAXCOLUMNNAMESIZE];
    int serial;

#if defined(HAVE_ICONV)
    /* Character encoding stuff */
    char *optinputcharset = NULL;
#endif

    strcpy(optvalidargs, STANDARDOPTS);
#if defined(HAVE_ICONV)
    /* Note that the declaration for optvalidargs currently reserves exactly
     * three chars for this value (two for the string, one for the trailing
     * \0). If you change this value, be sure to alter the optvalidargs
     * declaration accordingly! */
    strcat(optvalidargs, "s:");
#endif

    /* Attempt to parse any command line arguments */
    while((opt = getopt(argc, argv, optvalidargs)) != -1) {
        switch(opt) {
        case 'c':
            optusecreatetable = 1;
            optusetruncatetable = 0;
            break;
        case 'C':
            optusecreatetable = 0;
            break;
        case 'd':
            optusedroptable = 1;
            optusetruncatetable = 0;
            break;
        case 'D':
            optusedroptable = 0;
            break;
        case 'e':
            optuseifexists = 1;
            break;
        case 'E':
            optuseifexists = 0;
            break;
        case 'm':
            memofilename = optarg;
            break;
        case 'n':
            optnumericasnumeric = 1;
            break;
        case 'N':
            optnumericasnumeric = 0;
            break;
        case 'p':
            optshowprogress = 1;
            break;
        case 'P':
            optshowprogress = 0;
            break;
        case 'q':
            optusequotedtablename = 1;
            break;
        case 'Q':
            optusequotedtablename = 0;
            break;
#if defined(HAVE_ICONV)
        case 's':
            optinputcharset = optarg;
            break;
#endif
        case 't':
            optusetransaction = 1;
            break;
        case 'T':
            optusetransaction = 0;
            break;
        case 'u':
            optusetruncatetable = 1;
            optusecreatetable = 0;
            optusedroptable = 0;
            break;
        case 'U':
            optusetruncatetable = 0;
            break;
        case 'h':
        default:
            /* If we got here because someone requested '-h', exit
             * successfully.  Otherwise they used an invalid option, so
             * fail. */
            optexitcode = ((char) opt == 'h' ? EXIT_SUCCESS : EXIT_FAILURE);
        }
    }

    /* Checking that the user specified a filename, unless we're already
     * exiting for other reasons in which case it doesn't matter */
    if(optexitcode != EXIT_SUCCESS && optind > (argc - 1)) {
        optexitcode = EXIT_FAILURE;
    }

    if(optexitcode != -1) {
        printf(
#if defined(HAVE_ICONV)
               "Usage: %s [-cCdDeEhtTuU] [-s encoding] [-m memofilename] filename [indexcolumn ...]\n"
#else
               "Usage: %s [-cCdDeEhtTuU] [-m memofilename] filename [indexcolumn ...]\n"
#endif
               "Convert the named XBase file into PostgreSQL format\n"
               "\n"
               "  -c  issue a 'CREATE TABLE' command to create the table (default)\n"
               "  -C  do not issue a 'CREATE TABLE' command\n"
               "  -d  issue a 'DROP TABLE' command before creating the table (default)\n"
               "  -D  do not issue a 'DROP TABLE' command\n"
               "  -e  use 'IF EXISTS' when dropping tables (PostgreSQL 8.2+) (default)\n"
               "  -E  do not use 'IF EXISTS' when dropping tables (PostgreSQL 8.1 and older)\n"
               "  -h  print this message and exit\n"
               "  -m  the name of the associated memo file (if necessary)\n"
               "  -n  use type 'NUMERIC' for NUMERIC fields (default)\n"
               "  -N  use type 'TEXT' for NUMERIC fields\n"
               "  -p  show a progress bar during processing\n"
               "  -P  do not show a progress bar\n"
               "  -q  enclose the table name in quotation marks whenever used in statements\n"
               "  -Q  do not enclose the table name in quotation marks (default)\n"
#if defined(HAVE_ICONV)
               "  -s  the encoding used in the file, to be converted to UTF-8\n"
#endif
               "  -t  wrap a transaction around the entire series of statements (default)\n"
               "  -T  do not use an enclosing transaction\n"
               "  -u  issue a 'TRUNCATE' command before inserting data\n"
               "  -U  do not issue a 'TRUNCATE' command before inserting data (default)\n"
               "\n"
#if defined(HAVE_ICONV)
               "If you don't specify an encoding via '-s', the data will be printed as is.\n"
#endif
               "Using '-u' implies '-C -D'. Using '-c' or '-d' implies '-U'.\n"
               "\n"
               "%s is copyright 2008-2012 kirk@strauser.com.\n"
               "License GPLv3+: GNU GPL version 3 or later <http://gnu.org/licenses/gpl.html>\n"
               "This is free software: you are free to change and redistribute it.\n"
               "There is NO WARRANTY, to the extent permitted by law.\n"
               "Report bugs to <%s>\n", PACKAGE, PACKAGE_STRING, PACKAGE_BUGREPORT);
        exit(optexitcode);
    }

    /* Sanity check the arguments */
    if(!optusecreatetable) {
        /* It makes no sense to drop the table without creating it
         * afterward */
        optusedroptable = 0;
    }

#if defined(HAVE_ICONV)
    /* Initialize iconv */
    if(optinputcharset != NULL) {
        const char *outputcharset = "UTF-8";
        conv_desc = iconv_open(outputcharset, optinputcharset);

        if(conv_desc == (iconv_t)-1) {
            if(errno == EINVAL) {
                fprintf(stderr, "Conversion from '%s' to '%s' is not supported.\n", optinputcharset, outputcharset);
            } else {
                fprintf(stderr, "Initialization failure: %s\n", strerror(errno));
            }

            exit(1);
        }
    }
#endif

    /* Calculate the table's name based on the DBF filename */
    dbffilename = argv[optind];
    tablename = malloc(strlen(dbffilename) + 1);
    if(tablename == NULL) {
        exitwitherror("Unable to allocate the tablename buffer", 1);
    }
    /* The "bare" version of the tablename is the one used by itself in
     * lines line CREATE TABLE [...], etc. Compare this with tablename which
     * is used for other things, like creating the names of indexes. Despite
     * its name, baretablename may be surrounded by quote marks if the "-q"
     * option for optusequotedtablename is given. */
    baretablename = malloc(strlen(dbffilename) + 1 + optusequotedtablename * 2);
    if(baretablename == NULL) {
        exitwitherror("Unable to allocate the bare tablename buffer", 1);
    }
    /* Find the first character after the final slash, or the first
     * character of the filename if no slash is present, and copy from that
     * point to the period in the extension into the tablename string. */
    for(s = dbffilename + strlen(dbffilename) - 1; s != dbffilename; s--) {
        if(*s == '/') {
            s++;
            break;
        }
    }
    /* Create tablename and baretablename at the same time. */
    t = tablename;
    u = baretablename;
    if(optusequotedtablename) *u++ = '"';
    while(*s) {
        if(*s == '.') {
            break;
        }
        *t = tolower(*s++);
        *u++ = *t;
        t++;
    }
    if(optusequotedtablename) *u++ = '"';
    *t = '\0';
    *u = '\0';

    /* Get the DBF header */
    dbffile = fopen(dbffilename, "rb");
    if(dbffile == NULL) {
        exitwitherror("Unable to open the DBF file", 1);
    }
    if(setvbuf(dbffile, NULL, _IOFBF, DBFBATCHTARGET)) {
        exitwitherror("Unable to set the buffer for the dbf file", 1);
    }
    if(fread(&dbfheader, sizeof(dbfheader), 1, dbffile) != 1) {
        exitwitherror("Unable to read the entire DBF header", 1);
    }

    if(dbfheader.signature == 0x30) {
        /* Certain DBF files have an (empty?) 263-byte buffer after the header
         * information.  Take that into account when calculating field counts
         * and possibly seeking over it later. */
        skipbytes = 263;
    } else {
        skipbytes = 0;
    }

    /* Calculate the number of fields in this file */
    dbffieldsize = sizeof(DBFFIELD);
    fieldarraysize = littleint16_t(dbfheader.headerlength) - sizeof(dbfheader) - skipbytes - 1;
    if(fieldarraysize % dbffieldsize == 1) {
        /* Some dBASE III files include an extra terminator byte after the
         * field descriptor array.  If our calculations are one byte off,
         * that's the cause and we have to skip the extra byte when seeking
         * to the start of the records. */
        skipbytes += 1;
        fieldarraysize -= 1;
    } else if(fieldarraysize % dbffieldsize) {
        exitwitherror("The field array size is not an even multiple of the database field size", 0);
    }
    fieldcount = fieldarraysize / dbffieldsize;

    /* Fetch the description of each field */
    fields = malloc(fieldarraysize);
    if(fields == NULL) {
        exitwitherror("Unable to malloc the field descriptions", 1);
    }
    if(fread(fields, dbffieldsize, fieldcount, dbffile) != fieldcount) {
        exitwitherror("Unable to read all of the field descriptions", 1);
    }

    /* Keep track of PostgreSQL output parameters */
    pgfields = malloc(fieldcount * sizeof(PGFIELD));
    if(pgfields == NULL) {
        exitwitherror("Unable to malloc the output parameter list", 1);
    }
    for(i = 0; i < fieldcount; i++) {
        pgfields[i].formatstring = NULL;
    }

    /* Check for the terminator character */
    if(fread(&terminator, 1, 1, dbffile) != 1) {
        exitwitherror("Unable to read the terminator byte", 1);
    }
    if(terminator != 13) {
        exitwitherror("Invalid terminator byte", 0);
    }

    /* Skip the database container if necessary */
    if(fseek(dbffile, skipbytes, SEEK_CUR)) {
        exitwitherror("Unable to seek in the DBF file", 1);
    }

    /* Make sure we're at the right spot before continuing */
    if(ftell(dbffile) != littleint16_t(dbfheader.headerlength)) {
        exitwitherror("At an unexpected offset in the DBF file", 0);
    }

    /* Open the given memofile */
    if(memofilename != NULL) {
        memofd = open(memofilename, O_RDONLY);
        if(memofd == -1) {
            exitwitherror("Unable to open the memofile", 1);
        }
        if(fstat(memofd, &memostat) == -1) {
            exitwitherror("Unable to fstat the memofile", 1);
        }
        memofilesize = memostat.st_size;
        memomap = mmap(NULL, memofilesize, PROT_READ, MAP_PRIVATE, memofd, 0);
        if(memomap == MAP_FAILED) {
            exitwitherror("Unable to mmap the memofile", 1);
        }
        /* Rudimentary error checking. Make sure the "nextblock" field of
           the memofile's header isn't negative because that would be
           impossible. */
        memoheader = (MEMOHEADER*) memomap;
        memofileisdbase3 = dbfheader.signature == (int8_t) 0x83;
        if(memofileisdbase3) {
            memoblocknumber = slittleint32_t(memoheader->nextblock);
        } else {
            memoblocknumber = sbigint32_t(memoheader->nextblock);
        }
        if(memoblocknumber < 0) {
            exitwitherror("The next memofile block is negative. The specified "
                          "memofile probably isn't really a memofile.", 0);
        }
        if(memofileisdbase3) {
            memoblocksize = 512;
        } else {
            memoblocksize = (size_t) sbigint16_t(memoheader->blocksize);
        }
    }

    /* Encapsulate the whole process in a transaction */
    if(optusetransaction) {
        printf("BEGIN;\n");
    }

    /* Drop the table if requested */
    if(optusedroptable) {
        printf("SET statement_timeout=60000; DROP TABLE");
        /* Newer versions of PostgreSQL (8.2+) support "if exists" when
         * dropping tables. */
        if(optuseifexists) {
            printf(" IF EXISTS");
        }
        printf(" %s; SET statement_timeout=0;\n", baretablename);
    }

    /* Uniqify the XBase field names. It's possible to have multiple fields
     * with the same name, but PostgreSQL correctly considers that an error
     * condition. */
    if(optusecreatetable) {
        fieldnames = calloc(fieldcount, MAXCOLUMNNAMESIZE);
        if(fieldnames == NULL) {
            exitwitherror("Unable to allocate the columnname uniqification buffer", 1);
        }
        for(fieldnum = 0; fieldnum < fieldcount; fieldnum++) {
            /* Lowercase the field names to make PostgreSQL column names */
            s = fields[fieldnum].name;
            t = fieldnames[fieldnum];
            while(*s) {
                *t++ = tolower(*s++);
            }
            *t = '\0';
        }
        for(i = 1; i < fieldcount; i++) {
            /* Search for duplicates in all the previously processed field names */
            isuniquename = 1;
            for(j = 0; j < i; j++) {
                if(i != j && !strcmp(fieldnames[i], fieldnames[j])) {
                    isuniquename = 0;
                    break;
                }
            }
            /* No duplicates? Move on to the next. */
            if(isuniquename) {
                continue;
            }

            /* Create a unique name by appending "_" plus an ever-increasing
             * serial number to the end of the field name until it doesn't match
             * any other field name. */
            strcpy(basename, fieldnames[i]);
            serial = 2;
            while(!isuniquename) {
                /* sprintf() is safe because it's impossible for the longest XBase
                 * field name plus an underscore plus a serial number (which can't
                 * be greater than 4 digits long because of XBase field count
                 * limits) plus the trailing \0 to be longer than
                 * MAXCOLUMNNAMESIZE. */
                sprintf(fieldnames[i], "%s_%d", basename, serial);
                isuniquename = 1;
                for(j = 0; j < fieldcount; j++) {
                    if(j != i && !strcmp(fieldnames[i], fieldnames[j])) {
                        isuniquename = 0;
                        break;
                    }
                }
                serial++;
            }
        }
    }

    /* Generate the create table statement, do some sanity testing, and scan
     * for a few additional output parameters.  This is an ugly loop that
     * does lots of stuff, but extracting it into two or more loops with the
     * same structure and the same switch-case block seemed even worse. */
    if(optusecreatetable) printf("CREATE TABLE %s (", baretablename);
    printed = 0;
    for(fieldnum = 0; fieldnum < fieldcount; fieldnum++) {
        if(fields[fieldnum].type == '0') {
            continue;
        }
        if(printed && optusecreatetable) {
            if(optusecreatetable) printf(", ");
        }
        else {
            printed = 1;
        }

        if(optusecreatetable) {
            /* If the fieldname is a reserved word, rename it to start with
             * "tablename_" */
            isreservedname = 0;
            for(i = 0; RESERVEDWORDS[i]; i++ ) {
                if(!strcmp(fieldnames[fieldnum], RESERVEDWORDS[i])) {
                    printf("%s_%s ", tablename, fieldnames[fieldnum]);
                    isreservedname = 1;
                    break;
                }
            }
            if(!isreservedname) printf("%s ", fieldnames[fieldnum]);
        }

        switch(fields[fieldnum].type) {
        case 'B':
            /* Precalculate this field's format string so that it doesn't
             * have to be done inside the main loop */
            if(asprintf(&pgfields[fieldnum].formatstring, "%%.%dlf", fields[fieldnum].decimals) < 0) {
                exitwitherror("Unable to allocate a format string", 1);
            }
            if(optusecreatetable) printf("DOUBLE PRECISION");
            break;
        case 'C':
            if(optusecreatetable) printf("VARCHAR(%d)", fields[fieldnum].length);
            break;
        case 'D':
            if(optusecreatetable) printf("DATE");
            break;
        case 'F':
            if(fields[fieldnum].decimals > 0) {
                printf("NUMERIC(%d, %d)", fields[fieldnum].length, fields[fieldnum].decimals);
            } else {
                printf("NUMERIC(%d)", fields[fieldnum].length);
            }
            break;
        case 'G':
            if(optusecreatetable) printf("BYTEA");
            break;
        case 'I':
            if(optusecreatetable) printf("INTEGER");
            break;
        case 'L':
            /* This was a smallint at some point in the past */
            if(optusecreatetable) printf("BOOLEAN");
            break;
        case 'M':
            if(memofilename == NULL) {
                printf("\n");
                fprintf(stderr, "Table %s has memo fields, but couldn't open the related memo file\n", tablename);
                exit(EXIT_FAILURE);
            }
            if(optusecreatetable) printf("TEXT");
            /* Decide whether to use numeric or packed int memo block
             * number */
            if(fields[fieldnum].length == 4) {
                pgfields[fieldnum].memonumbering = PACKEDMEMOSTYLE;
            } else if (fields[fieldnum].length == 10) {
                pgfields[fieldnum].memonumbering = NUMERICMEMOSTYLE;
            } else {
                exitwitherror("Unknown memo record number style", 0);
            }
            break;
        case 'N':
            if(optusecreatetable) {
                if(optnumericasnumeric) {
                    if(fields[fieldnum].decimals > 0) {
                        printf("NUMERIC(%d, %d)", fields[fieldnum].length, fields[fieldnum].decimals);
                    } else {
                        printf("NUMERIC(%d)", fields[fieldnum].length);
                    }
                } else {
                    printf("TEXT");
                }
            }
            break;
        case 'T':
            if(optusecreatetable) printf("TIMESTAMP");
            break;
        case 'Y':
            if(optusecreatetable) printf("DECIMAL(20,4)");
            break;
        default:
            if(optusecreatetable) printf("\n");
            fprintf(stderr, "Unhandled field type: %c\n", fields[fieldnum].type);
            exit(EXIT_FAILURE);
        }
        if(fields[fieldnum].length > longestfield) {
            longestfield = fields[fieldnum].length;
        }
    }
    if(optusecreatetable) printf(");\n");

    /* Truncate the table if requested */
    if(optusetruncatetable) {
        printf("TRUNCATE TABLE %s;\n", baretablename);
    }

    /* Get PostgreSQL ready to receive lots of input */
    printf("\\COPY %s FROM STDIN\n", baretablename);

    dbfbatchsize = DBFBATCHTARGET / littleint16_t(dbfheader.recordlength);
    if(!dbfbatchsize) {
        dbfbatchsize = 1;
    }
    inputbuffer = malloc(littleint16_t(dbfheader.recordlength) * dbfbatchsize);
    if(inputbuffer == NULL) {
        exitwitherror("Unable to malloc a record buffer", 1);
    }
    outputbuffer = malloc(longestfield + 1);
    if(outputbuffer == NULL) {
        exitwitherror("Unable to malloc the output buffer", 1);
    }

    /* Loop across records in the file, taking 'dbfbatchsize' at a time, and
     * output them in PostgreSQL-compatible format */
    if(optshowprogress) {
        fprintf(stderr, "Progress: 0");
        fflush(stderr);
    }
    for(recordbase = 0; recordbase < littleint32_t(dbfheader.recordcount); recordbase += dbfbatchsize) {
        blocksread = fread(inputbuffer, littleint16_t(dbfheader.recordlength), dbfbatchsize, dbffile);
        if(blocksread != dbfbatchsize &&
           recordbase + blocksread < littleint32_t(dbfheader.recordcount)) {
            exitwitherror("Unable to read an entire record", 1);
        }
        for(batchindex = 0; batchindex < blocksread; batchindex++) {
            bufoffset = inputbuffer + littleint16_t(dbfheader.recordlength) * batchindex;
            /* Skip deleted records */
            if(bufoffset[0] == '*') {
                continue;
            }
            bufoffset++;
            for(fieldnum = 0; fieldnum < fieldcount; fieldnum++) {
                if(fields[fieldnum].type == '0') {
                    continue;
                }
                if(fieldnum) {
                    printf("\t");
                }
                switch(fields[fieldnum].type) {
                case 'B':
                    /* Double floats */
                    printf(pgfields[fieldnum].formatstring, sdouble(bufoffset));
                    break;
                case 'C':
                    /* Varchars */
                    safeprintbuf(bufoffset, fields[fieldnum].length);
                    break;
                case 'D':
                    /* Datestamps */
                    if(bufoffset[0] == ' ' || bufoffset[0] == '\0') {
                        printf("\\N");
                    } else {
                        s = outputbuffer;
                        *s++ = bufoffset[0];
                        *s++ = bufoffset[1];
                        *s++ = bufoffset[2];
                        *s++ = bufoffset[3];
                        *s++ = '-';
                        *s++ = bufoffset[4];
                        *s++ = bufoffset[5];
                        *s++ = '-';
                        *s++ = bufoffset[6];
                        *s++ = bufoffset[7];
                        *s++ = '\0';
                        printf("%s", outputbuffer);
                    }
                    break;
                case 'G':
                    /* General binary objects */
                    /* This is left unimplemented to avoid breakage for
                     * people porting databases with OLE objects, at least
                     * until someone comes up with a good way to display
                     * them. */
                    break;
                case 'I':
                    /* Integers */
                    printf("%d", slittleint32_t(bufoffset));
                    break;
                case 'L':
                    /* Booleans */
                    switch(bufoffset[0]) {
                    case 'Y':
                    case 'T':
                        putchar('t');
                        break;
                    default:
                        putchar('f');
                        break;
                    }
                    break;
                case 'M':
                    /* Memos */
                    if(pgfields[fieldnum].memonumbering == PACKEDMEMOSTYLE) {
                        memoblocknumber = slittleint32_t(bufoffset);
                    } else {
                        memoblocknumber = 0;
                        s = bufoffset;
                        for(i = 0; i < 10; i++) {
                            if(*s && *s != 32) {
                                /* I'm unaware of any non-ASCII
                                 * implementation of XBase. */
                                memoblocknumber = memoblocknumber * 10 + *s - '0';
                            }
                            s++;
                        }
                    }
                    if(memoblocknumber) {
                        memorecordoffset = memoblocksize * memoblocknumber;
                        if(memorecordoffset >= memofilesize) {
                            exitwitherror("A memo record past the end of the memofile was requested", 0);
                        }
                        memorecord = memomap + memorecordoffset;
                        if(memofileisdbase3) {
                            t = strchr(memorecord, 0x1A);
                            safeprintbuf(memorecord, t - memorecord);
                        } else {
                            safeprintbuf(memorecord + 8, sbigint32_t(memorecord + 4));
                        }
                    }
                    break;
                case 'F':
                case 'N':
                    /* Numerics */
                    strncpy(outputbuffer, bufoffset, fields[fieldnum].length);
                    outputbuffer[fields[fieldnum].length] = '\0';
                    /* Strip off *leading* spaces */
                    s = outputbuffer;
                    while(*s == ' ') {
                        s++;
                    }
                    if(*s == '\0') {
                        printf("\\N");
                    } else {
                        printf("%s", s);
                    }
                    break;
                case 'T':
                    /* Timestamps */
                    juliandays = slittleint32_t(bufoffset);
                    seconds = (slittleint32_t(bufoffset + 4) + 1) / 1000;
                    if(!(juliandays || seconds)) {
                        printf("\\N");
                    } else {
                        hours = seconds / 3600;
                        seconds -= hours * 3600;
                        minutes = seconds / 60;
                        seconds -= minutes * 60;
                        printf("J%d %02d:%02d:%02d", juliandays, hours, minutes, seconds);
                    }
                    break;
                case 'Y':
                    /* Currency */
                    t = outputbuffer + sprintf(outputbuffer, "%05"PRId64, slittleint64_t(bufoffset));
                    *(t + 1) = '\0';
                    *(t) = *(t - 1);
                    *(t - 1) = *(t - 2);
                    *(t - 2) = *(t - 3);
                    *(t - 3) = *(t - 4);
                    *(t - 4) = '.';
                    printf("%s", outputbuffer);
                    break;
                };
                bufoffset += fields[fieldnum].length;
            }
            printf("\n");
        }
        if(optshowprogress) {
            updateprogressbar(100 * (recordbase + blocksread) / littleint32_t(dbfheader.recordcount));
        }
    }
    if(optshowprogress) { updateprogressbar(100); }
    free(inputbuffer);
    free(outputbuffer);
    printf("\\.\n");

    /* Until this point, no changes have been flushed to the database */
    if(optusetransaction) {
        printf("COMMIT;\n");
    }

    /* Generate the indexes */
    for(i = optind + 1; i < argc; i++ ){
        printf("CREATE INDEX %s_", tablename);
        for(s = argv[i]; *s; s++) {
            if(isalnum(*s)) {
                putchar(*s);
                lastcharwasreplaced = 0;
            } else {
                /* Only output one underscore in a row */
                if(!lastcharwasreplaced) {
                    putchar('_');
                    lastcharwasreplaced = 1;
                }
            }
        }
        printf(" ON %s(%s);\n", baretablename, argv[i]);
    }

    free(tablename);
    free(baretablename);
    free(fields);
    for(fieldnum = 0; fieldnum < fieldcount; fieldnum++) {
        if(pgfields[fieldnum].formatstring != NULL) {
            free(pgfields[fieldnum].formatstring);
        }
    }
    free(pgfields);
    fclose(dbffile);
    if(memomap != NULL) {
        if(munmap(memomap, memostat.st_size) == -1) {
            exitwitherror("Unable to munmap the memofile", 1);
        }
        close(memofd);
    }

#if defined(HAVE_ICONV)
    if(conv_desc != NULL) {
        if(iconv_close(conv_desc) != 0) {
            fprintf(stderr, "iconv_close failed: %s\n", strerror(errno));
            exit(1);
        }
    }
#endif

    return 0;
}
