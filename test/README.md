# Creating test cases

Publicly distributable test cases go in the `test/cases` directory. Put private and/or confidential cases in `test/privatecases`, which Git ignores.

Within those directories, place your database files into the `data` subdirectory.

Test cases are JSON files named `[something].json` with defined keys:

* **cmd_args**: the arguments passed to pgdbf to generate the test output. If this is a string, it will be passed as a single argument to pgdbf. If you're attempting to pass more than one argument, *this is probably not what you want*. In that case, set `cmd_args` to a list of string arguments.
    * With one argument: `"cmd_args": "mytable.dbf"`
    * With multiple arguments: `"cmd_args": ["-m", "memofile.fpt", "datafile.dbf"]`
* **head**: a string to be matched against the start of the test output
* **length**: the expected length of the test output
* **md5**: the expected MD5 hex digest of the test output
* **tail**: a string to be matched against the start of the test output

Unknown keys are ignored.

Public test cases are loaded and executed in alphabetical order, followed by private test cases.

# Running test cases

Run the `runtests.py` program *from within the `test/` directory* to execute all unit tests. Use the `-p` argument to select a different pgdbf executable to test (such as a freshly built on at `src/pgdbf`). By default, all test cases in `test/cases` and `test/privatecases` are executed in alphabetical order. If you specify one or more test cases as command line arguments, only those test cases will be executed.

Before each test case is executed, `runtests.py` CDs into the directory containing that test case. This way, relative paths to data files (like `data/my-database.dbf`) work.

Example:

    ./runtests.py ../src/pgdbf

will test a freshly built-but-not-installed binary with all test cases in `test/cases` and `test/privatecases`.

    ./runtests.py my-test-case.json

will test the first pgdbf executable in `$PATH` with just `my-test-case.json`.

# Python 3 compatibility

Of course. :-)
