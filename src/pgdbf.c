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
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "pgdbf.h"

int main(int argc, char **argv)
{
    /* Describing the DBF file */
    char          *dbffilename;
    FILE          *dbffile;
    DBFHEADER      dbfheader;
    DBFFIELD      *fields;
    PGFIELD       *pgfields;
    size_t         dbffieldsize;
    size_t         fieldcount;	   /* Number of fields for this DBF file */
    unsigned int   recordbase;	   /* The first record in a batch of records */
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
    char        *memofilename;
    int          memofd;
    struct stat  memostat;
    int32_t      memoblocknumber;

    void        *memomap = NULL; /* Pointer to the mmap of the memo file */
    void        *memorecord;	 /* Pointer to the current memo block */
    size_t       memoblocksize = 0;  /* The length of each memo block */

    /* Processing and misc */
    char *inputbuffer;
    char *outputbuffer;
    char *bufoffset;
    char *s;
    char *t;
    int  lastcharwasreplaced = 0;

    /* Datetime calculation stuff */
    int32_t juliandays;
    int32_t seconds;
    int     hours;
    int     minutes;

    int     i;
    int     isreservedname;
    int     printed;
    size_t  blocksread;
    size_t  longestfield = 32;  /* Make sure we leave at least enough room
				 * to print out long formatted numbers, like
				 * currencies. */

    /* Command line option parsing */
    int     opt;
    int     optexitcode = -1;	/* Left at -1 means that the arguments were
				 * valid and the program should run.
				 * Anything else is an exit code and the
				 * program will stop. */
    int     usecreatetable = 1;
    int     usedroptable = 1;
    int     useifexists = 0;
    int     usetransaction = 1;

    /* Describing the PostgreSQL table */
    char *tablename;
    char  fieldname[11];

    /* Attempt to parse any command line arguments */
    while((opt = getopt(argc, argv, "cCdDehtT")) != -1) {
	switch(opt) {
	case 'c':
	    usecreatetable = 1;
	    break;
	case 'C':
	    usecreatetable = 0;
	    break;
	case 'd':
	    usedroptable = 1;
	    break;
	case 'D':
	    usedroptable = 0;
	    break;
	case 'e':
	    useifexists = 1;
	    break;
	case 't':
	    usetransaction = 1;
	    break;
	case 'T':
	    usetransaction = 0;
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
	printf("Usage: %s [-cCdDehtT] filename [indexcolumn ...]\n", PACKAGE);
	printf("Convert the named XBase file into PostgreSQL format\n");
	printf("\n");
	printf("  -c  issue a 'CREATE TABLE' command to create the table (default)\n");
	printf("  -C  do not issue a 'CREATE TABLE' command\n");
	printf("  -d  issue a 'DROP TABLE' command before creating the table (default)\n");
	printf("  -D  do not issue a 'DROP TABLE' command\n");
	printf("  -e  use 'IF EXISTS' when dropping tables (PostgreSQL 8.2+)\n");
	printf("  -h  print this message and exit\n");
	printf("  -t  wrap a transaction around the entire series of statements (default)\n");
	printf("  -T  do not use an enclosing transaction\n");
	printf("\n");
	printf("%s is copyright 2009 The Day Companies.\n", PACKAGE_STRING);
	printf("License GPLv3+: GNU GPL version 3 or later <http://gnu.org/licenses/gpl.html>\n");
	printf("This is free software: you are free to change and redistribute it.\n");
	printf("There is NO WARRANTY, to the extent permitted by law.\n");
	printf("Report bugs to <%s>\n", PACKAGE_BUGREPORT);
	exit(optexitcode);
    }

    /* Sanity check the arguments */
    if(!usecreatetable) {
	/* It makes no sense to drop the table without creating it
	 * afterward */
	usedroptable = 0;
    }

    /* Calculate the table's name based on the DBF filename */
    dbffilename = argv[optind];
    tablename = malloc(strlen(dbffilename) + 1);
    if(tablename == NULL) {
	exitwitherror("Unable to allocate the tablename buffer", 1);
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
    t = tablename;
    while(*s) {
	if(*s == '.') {
	    break;
	}
	*t++ = tolower(*s++);
    }
    *t = '\0';

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
	/* Certain Visual FoxPro files have an (empty?) 263-byte buffer
	 * after the header information.  Take that into account when
	 * calculating field counts and possibly seeking over it later. */
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

    /* The memofile's name is the same as the DBF file's name, but ending in
     * .fpt */
    memofilename = malloc(strlen(dbffilename) + 4);
    if(memofilename == NULL) {
	exitwitherror("Unable to allocate the memo filename buffer", 1);
    }
    strcpy(memofilename, dbffilename);
    for(s = memofilename + strlen(memofilename) - 1;
	*s != '.' && s != memofilename;
	s--);
    s++;
    strcpy(s, "fpt");
    memofd = open(memofilename, O_RDONLY);
    if(memofd != -1) {
	if (fstat(memofd, &memostat) == -1) {
	    exitwitherror("Unable to fstat the memofile", 1);
	}
	memomap = mmap(NULL, memostat.st_size, PROT_READ, MAP_PRIVATE, memofd, 0);
	if(memomap == MAP_FAILED) {
	    exitwitherror("Unable to mmap the memofile", 1);
	}
	memoblocksize = (size_t) sbigint16_t(((MEMOHEADER*) memomap)->blocksize);
    }

    /* Encapsulate the whole process in a transaction */
    if(usetransaction) {
	printf("BEGIN;\n");
    }

    /* Drop the table if requested */
    if(usedroptable) {
	printf("SET statement_timeout=60000; DROP TABLE");
	/* Newer versions of PostgreSQL (8.2+) support "if exists" when
	 * dropping tables. */
	if(useifexists) {
	    printf(" IF EXISTS");
	}
	printf(" %s; SET statement_timeout=0;\n", tablename);
    }

    /* Generate the create table statement, do some sanity testing, and scan
     * for a few additional output parameters.  This is an ugly loop that
     * does lots of stuff, but extracting it into two or more loops with the
     * same structure and the same switch-case block seemed even worse. */
    if(usecreatetable) printf("CREATE TABLE %s (", tablename);
    printed = 0;
    for(fieldnum = 0; fieldnum < fieldcount; fieldnum++) {
	if(fields[fieldnum].type == '0') {
	    continue;
	}
	if(printed && usecreatetable) {
	    if(usecreatetable) printf(", ");
	}
	else {
	    printed = 1;
	}

	s = fields[fieldnum].name;
	t = fieldname;
	while(*s) {
	    *t++ = tolower(*s++);
	}
	*t = '\0';
	    
	 /* If the fieldname is a reserved word, rename it to start with */
	 /* "tablename_" */
	isreservedname = 0;
	for(i = 0; RESERVEDWORDS[i]; i++ ) {
	    if(!strcmp(fieldname, RESERVEDWORDS[i])) {
		if(usecreatetable) printf("%s_%s ", tablename, fieldname);
		isreservedname = 1;
		break;
	    }
	}
	if(!isreservedname) {
	    if(usecreatetable) printf("%s ", fieldname);
	}
	switch(fields[fieldnum].type) {
	case 'B':
	    /* Precalculate this field's format string so that it doesn't
	     * have to be done inside the main loop */
	    if(asprintf(&pgfields[fieldnum].formatstring, "%%.%dlf", fields[fieldnum].decimals) < 0) {
		exitwitherror("Unable to allocate a format string", 1);
	    }
	    if(usecreatetable) printf("DOUBLE PRECISION");
	    break;
	case 'C':
	    if(usecreatetable) printf("VARCHAR(%d)", fields[fieldnum].length);
	    break;
	case 'D':
	    if(usecreatetable) printf("DATE");
	    break;
	case 'F':
	    if(usecreatetable) printf("NUMERIC(%d)", fields[fieldnum].decimals);
	    break;
	case 'G':
	    if(usecreatetable) printf("BYTEA");
	    break;
	case 'I':
	    if(usecreatetable) printf("INTEGER");
	    break;
	case 'L':
	    /* This was a smallint at some point in the past */
	    if(usecreatetable) printf("BOOLEAN");	    break;
	case 'M':
	    if(memofd == -1) {
		printf("\n");
		fprintf(stderr, "Table %s has memo fields, but couldn't open the related memo file\n", tablename);
		exit(EXIT_FAILURE);
	    }
	    if(usecreatetable) printf("TEXT");
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
	    /* Was a numeric at one point, but for our purposes a text field
	     * is better because there isn't a perfect overlap between
	     * FoxPro and PostgreSQL numeric types */
	    if(usecreatetable) printf("TEXT");
	    break;
	case 'T':
	    if(usecreatetable) printf("TIMESTAMP");
	    break;
	case 'Y':
	    if(usecreatetable) printf("DECIMAL(4)");
	    break;
	default:
	    if(usecreatetable) printf("\n");
	    fprintf(stderr, "Unhandled field type: %c\n", fields[fieldnum].type);
	    exit(EXIT_FAILURE);
	}
	if(fields[fieldnum].length > longestfield) {
	    longestfield = fields[fieldnum].length;
	}
    }
    if(usecreatetable) printf(");\n");

    /* Get PostgreSQL ready to receive lots of input */
    printf("\\COPY %s FROM STDIN\n", tablename);

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
		       people porting databases with OLE objects, at least
		       until someone comes up with a good way to display
		       them. */
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
			    if(*s != 32) {
				/* I'm unaware of any non-ASCII
				   implementation of XBase. */
				memoblocknumber = memoblocknumber * 10 + *s - '0';
			    }
			    s++;
			}
		    }
		    if(memoblocknumber) {
			memorecord = memomap + memoblocksize * memoblocknumber;
			safeprintbuf(memorecord + 8, sbigint32_t(memorecord + 4));
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
		    printf("%s", s);
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
		    t = outputbuffer + sprintf(outputbuffer, "%05jd", slittleint64_t(bufoffset));
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
    }
    free(inputbuffer);
    free(outputbuffer);
    printf("\\.\n");

    /* Until this point, no changes have been flushed to the database */
    if(usetransaction) {
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
	printf(" ON %s(%s);\n", tablename, argv[i]);
    }

    free(tablename);
    free(memofilename);
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
    return 0;
}
