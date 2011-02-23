#!/usr/bin/env python

import Utils

top = '.'
out = 'build'

def options (ctx):
	ctx.load('compiler_c')

	ctx.add_option('--otf', action='store', default=None, help='Use OTF')

def configure (ctx):
	ctx.load('compiler_c')
	ctx.load('gnu_dirs')

	ctx.check_cfg(
		package = 'glib-2.0',
		args = ['--cflags', '--libs'],
		uselib_store = 'GLIB'
	)

	ctx.check_cfg(
		package = 'gobject-2.0',
		args = ['--cflags', '--libs'],
		uselib_store = 'GOBJECT'
	)

	ctx.check_cfg(
		package = 'gio-2.0',
		args = ['--cflags', '--libs'],
		uselib_store = 'GIO'
	)

	ctx.check_cfg(
		package = 'gmodule-2.0',
		args = ['--cflags', '--libs'],
		uselib_store = 'GMODULE'
	)

	if ctx.options.otf:
		ctx.env.LIB_OTF      = ['otf']
		ctx.env.LIBPATH_OTF  = ['%s/lib' % (ctx.options.otf,)]
		ctx.env.RPATH_OTF    = ['%s/lib' % (ctx.options.otf,)]
		ctx.env.INCLUDES_OTF = ['%s/include' % (ctx.options.otf,)]

		ctx.check_cc(
			header_name = 'otf.h',
			define_name = 'HAVE_OTF',
			use = ['OTF']
		)

		ctx.check_cc(
			lib = 'otf',
			define_name = 'HAVE_OTF',
			use = ['OTF']
		)

	ctx.env.CFLAGS += ['-std=c99', '-pedantic', '-Wall', '-Wextra']
	ctx.env.CFLAGS += ['-Wno-missing-field-initializers', '-Wno-unused-parameter', '-Wold-style-definition', '-Wdeclaration-after-statement', '-Wmissing-declarations', '-Wmissing-prototypes', '-Wredundant-decls', '-Wmissing-noreturn', '-Wshadow', '-Wpointer-arith', '-Wcast-align', '-Wwrite-strings', '-Winline', '-Wformat-nonliteral', '-Wformat-security', '-Wswitch-enum', '-Wswitch-default', '-Winit-self', '-Wmissing-include-dirs', '-Wundef', '-Waggregate-return', '-Wmissing-format-attribute', '-Wnested-externs', '-Wstrict-prototypes']

	ctx.env.CFLAGS += ['-D_FILE_OFFSET_BITS=64']

def build (ctx):
	ctx.stlib(
		source = ['julea/%s.c' % file for file in ('jbson', 'jbson-iterator', 'jcollection', 'jcollection-iterator', 'jcommon', 'jconnection', 'jcredentials', 'jdistribution', 'jerror', 'jitem', 'jlist', 'jlist-iterator', 'jmongo', 'jmongo-connection', 'jmongo-iterator', 'jmongo-message', 'jmongo-reply', 'jobjectid', 'jsemantics', 'jstore', 'jstore-iterator')] + ['common/%s.c' % file for file in ('jconfiguration', 'jmessage')],
		target = 'julea',
		use = ['GLIB', 'GOBJECT', 'GIO'],
		includes = ['common'],
		install_path = None
	)

	for test in ('bson', 'bson-iterator', 'distribution', 'list', 'list-iterator', 'semantics'):
		ctx.program(
			source = ['test/%s.c' % (test,)],
			target = 'test/%s' % (test,),
			use = ['GLIB', 'julea'],
			includes = ['common', 'julea'],
			install_path = None
		)

	ctx.program(
		source = ['julead/%s.c' % file for file in ('julead',)] + ['common/%s.c' % file for file in ('jconfiguration', 'jmessage')],
		target = 'julead/julead',
		use = ['GLIB', 'GOBJECT', 'GIO', 'GMODULE', 'OTF'],
		includes = ['common'],
		defines = ['JULEAD_BACKEND_PATH="%s"' % (Utils.subst_vars('${LIBDIR}/julea/backend', ctx.env),)],
		install_path = '${BINDIR}'
	)

	for backend in ('gio', 'null', 'posix'):
		ctx.shlib(
			source = ['julead/backend/%s.c' % (backend,)] + ['common/%s.c' % file for file in ('jconfiguration', 'jmessage')],
			target = 'julead/backend/%s' % (backend,),
			use = ['GLIB', 'GOBJECT', 'GIO', 'GMODULE', 'OTF'],
			includes = ['common'],
			install_path = '${LIBDIR}/julea/backend'
		)

	for tool in ('julea-config',):
		ctx.program(
			source = ['tools/%s.c' % (tool,)],
			target = 'tools/%s' % (tool,),
			use = ['GLIB', 'GOBJECT', 'GIO'],
			install_dir = '${BINDIR}'
		)

	ctx.program(
		source = ['benchmark.c'],
		target = 'benchmark',
		use = ['GLIB', 'GOBJECT', 'julea'],
		includes = ['common', 'julea'],
		install_path = None
	)
