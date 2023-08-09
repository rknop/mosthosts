AC_INIT( [mosthosts_viewthing], [0.1], [raknop@lbl.gov] )
AM_INIT_AUTOMAKE
AC_CONFIG_FILES([
  Makefile
  src/Makefile
])

AC_ARG_WITH( webapdir,
             [AS_HELP_STRING([--with-webapdir=WEBAPDIR], [Where to install webap] )],
             [webapdir=$withval],
             [webapdir=${prefix}/html] )

AC_ARG_WITH( sessiondir,
             [AS_HELP_STRING([--with-sessiondir=WEBAPDIR], [Where webap stores sessions] )],
             [sessiondir=$withval],
             [sessiondir=${prefix}/sessions] )

AC_SUBST(webapdir)
AC_SUBST(sessiondir)
AC_OUTPUT
