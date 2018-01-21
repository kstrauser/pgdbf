#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.62)
AC_INIT([PgDBF], [0.6.3], [kirk@strauser.com])
AM_INIT_AUTOMAKE([-Wall -Werror foreign])
AC_CONFIG_SRCDIR([config.h.in])
AC_CONFIG_HEADERS([config.h])

# Checks for programs.
AC_PROG_CC

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h stdint.h stdlib.h string.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_INT16_T
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_INT8_T
AC_TYPE_SIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT8_T
AC_C_BIGENDIAN

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_MMAP
AC_CHECK_FUNCS([munmap strchr])

AC_ARG_ENABLE([iconv], AC_HELP_STRING([--disable-iconv], [do not use iconv() at all]),
              ac_arg_use_iconv=$enableval, ac_arg_use_iconv=yes)
AC_ARG_ENABLE([installdocs],
[  --enable-installdocs    install documentation],
[case "${enableval}" in
  yes) installdocs=true ;;
  no)  installdocs=false ;;
  *) AC_MSG_ERROR([bad value ${enableval} for --enable-installdocs]) ;;
esac],[installdocs=true])
AM_CONDITIONAL([INSTALLDOCS], [test x$installdocs = xtrue])

if test "x$ac_arg_use_iconv" != "xno"; then
  AM_ICONV
fi

AC_CONFIG_FILES([Makefile
                 doc/Makefile
                 src/Makefile])
AC_OUTPUT
