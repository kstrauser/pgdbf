#!/usr/bin/env python

# pylint: disable=superfluous-parens

"""Run a suite of PgDBF test cases."""

import argparse
from glob import glob
from hashlib import md5
from json import load
from logging import basicConfig, getLogger, DEBUG, INFO
from os import chdir, getcwd
from subprocess import Popen, PIPE, STDOUT

LOGGER = getLogger('')

class TestError(ValueError):
    """A test failed"""


def check_head(expected):
    """Check that the start of the file is as expected"""

    LOGGER.debug('opened a header check')
    body = bytes()
    length = len(expected)
    while True:
        data = yield()
        if not data:
            raise ValueError({'error': 'short read', 'expected': length, 'actual': len(body)})
        body += data
        if len(body) >= length:
            actual = body[:length]
            if expected != actual.decode():
                raise TestError('unequal head', expected, actual)
            LOGGER.info('passed the header check')
            break

    while True:
        data = yield()
        if data is None:
            LOGGER.debug('closed the header check')
            return


def check_length(expected):
    """Check that the file has the expected length"""

    LOGGER.debug('opened a length check')
    actual = 0
    while True:
        data = yield()
        if not data:
            if expected != actual:
                raise TestError('incorrect length', expected, actual)
            LOGGER.info('passed the length check')
            LOGGER.debug('closed the length check')
            return
        actual += len(data)


def check_md5(expected):
    """Check that the file has the expected MD5 hash"""

    LOGGER.debug('opened an md5 check')
    hasher = md5()
    while True:
        data = yield()
        if data is None:
            actual = hasher.hexdigest()
            if expected != actual:
                raise TestError('bad md5 hash', actual, expected)
            LOGGER.info('passed the md5 check')
            LOGGER.debug('closed the md5 check')
            return
        hasher.update(data)


def check_tail(expected):
    """Check that the end of the file is as expected"""

    LOGGER.debug('opened a tail check')
    actual = bytes()
    length = len(expected)
    while True:
        data = yield()
        if data is None:
            if expected != actual.decode():
                raise TestError('incorrect tail', actual, expected)
            LOGGER.info('passed the tail check')
            LOGGER.debug('closed the tail check')
            return
        actual = (actual + data)[-length:]


def run_test(pgdbf_path, config):
    """Run a test case with the given pgdbf executable"""

    tests = []
    for key, value in config.items():
        try:
            test_func = {
                'head': check_head,
                'length': check_length,
                'md5': check_md5,
                'tail': check_tail,
            }[key]
        except KeyError:
            pass
        else:
            test = test_func(value)
            next(test)
            tests.append(test)

    if not tests:
        raise ValueError('No tests are configured')

    args = config['cmd_args']
    if not isinstance(args, list):
        args = [args]
    command = Popen([pgdbf_path] + args, stdout=PIPE, stderr=STDOUT)
    while True:
        chunk = command.stdout.read(128 * 1024)
        if not chunk:
            break
        for test in tests:
            test.send(chunk)

    for test in tests:
        try:
            test.send(None)
        except StopIteration:
            pass
        else:
            raise ValueError('test {} did not close cleanly'.format(test))


def handle_command_line():
    """Evaluate the command line arguments and run tests"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pgdbf', '-p', help='Path to the pgdbf executable')
    parser.add_argument('--verbose', '-v', action='count', default=0,
                        help='Increase debugging verbosity')
    args = parser.parse_args()
    if args.verbose >= 2:
        basicConfig(level=DEBUG)
    elif args.verbose == 1:
        basicConfig(level=INFO)
    else:
        basicConfig()

    orig_dir = getcwd()
    for test_dir in ('cases', 'privatecases'):
        chdir(test_dir)
        for case in glob('*.json'):
            print('Running {}/{}'.format(test_dir, case))
            with open(case) as infile:
                run_test(args.pgdbf or 'pgdbf', load(infile))
        chdir(orig_dir)


if __name__ == '__main__':
    handle_command_line()
