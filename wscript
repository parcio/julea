#!/usr/bin/env python

import Utils

top = '.'
out = 'build'

def options (ctx):
	ctx.load('compiler_c')

	ctx.add_option('--hdtrace', action='store', default=None, help='Use HDTrace')
	ctx.add_option('--otf', action='store', default=None, help='Use OTF')
	ctx.add_option('--zookeeper', action='store', default=None, help='Use ZooKeeper')

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

	ctx.check_cfg(
		package = 'fuse',
		args = ['--cflags', '--libs'],
		uselib_store = 'FUSE',
		mandatory = False
	)

	if ctx.options.hdtrace:
		ctx.env.LIB_HDTRACE      = ['hdTracing']
		ctx.env.LIBPATH_HDTRACE  = ['%s/lib' % (ctx.options.hdtrace,)]
		ctx.env.RPATH_HDTRACE    = ['%s/lib' % (ctx.options.hdtrace,)]
		ctx.env.INCLUDES_HDTRACE = ['%s/include' % (ctx.options.hdtrace,)]

		ctx.check_cc(
			header_name = 'hdTrace.h',
			define_name = 'HAVE_HDTRACE',
			use = ['HDTRACE']
		)

		ctx.check_cc(
			lib = 'hdTracing',
			define_name = 'HAVE_HDTRACE',
			use = ['HDTRACE']
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

	if ctx.options.zookeeper:
		ctx.env.LIB_ZOOKEEPER      = ['zookeeper_mt']
		ctx.env.LIBPATH_ZOOKEEPER  = ['%s/lib' % (ctx.options.zookeeper,)]
		ctx.env.RPATH_ZOOKEEPER    = ['%s/lib' % (ctx.options.zookeeper,)]
		ctx.env.INCLUDES_ZOOKEEPER = ['%s/include/c-client-src' % (ctx.options.zookeeper,)]

		ctx.check_cc(
			header_name = 'zookeeper.h',
			define_name = 'HAVE_ZOOKEEPER',
			use = ['ZOOKEEPER']
		)

		ctx.check_cc(
			lib = 'zookeeper_mt',
			define_name = 'HAVE_ZOOKEEPER',
			use = ['ZOOKEEPER']
		)

	ctx.env.CFLAGS += ['-std=c99', '-pedantic', '-Wall', '-Wextra']
	ctx.env.CFLAGS += ['-Wno-missing-field-initializers', '-Wno-unused-parameter', '-Wold-style-definition', '-Wdeclaration-after-statement', '-Wmissing-declarations', '-Wmissing-prototypes', '-Wredundant-decls', '-Wmissing-noreturn', '-Wshadow', '-Wpointer-arith', '-Wcast-align', '-Wwrite-strings', '-Winline', '-Wformat-nonliteral', '-Wformat-security', '-Wswitch-enum', '-Wswitch-default', '-Winit-self', '-Wmissing-include-dirs', '-Wundef', '-Waggregate-return', '-Wmissing-format-attribute', '-Wnested-externs', '-Wstrict-prototypes']

	ctx.env.CFLAGS += ['-D_FILE_OFFSET_BITS=64']

def build (ctx):
	ctx.install_files('${INCLUDEDIR}/julea', ctx.path.ant_glob('include/*.h'))

	ctx.shlib(
		source = ctx.path.ant_glob('lib/*.c'),
		target = 'julea',
		use = ['GLIB', 'GOBJECT', 'GIO', 'HDTRACE', 'OTF'],
		includes = ['include'],
		install_path = None
	)

	for test in ('bson', 'bson-iterator', 'distribution', 'list', 'list-iterator', 'semantics'):
		ctx.program(
			source = ['test/%s.c' % (test,)],
			target = 'test/%s' % (test,),
			use = ['GLIB', 'julea'],
			includes = ['include', 'lib'],
			install_path = None
		)

	ctx.program(
		source = ['julead/%s.c' % file for file in ('julead',)],
		target = 'julead/julead',
		use = ['GLIB', 'GOBJECT', 'GIO', 'GMODULE', 'julea'],
		includes = ['include'],
		defines = ['JULEAD_BACKEND_PATH="%s"' % (Utils.subst_vars('${LIBDIR}/julea/backend', ctx.env),)],
		install_path = '${BINDIR}'
	)

	for backend in ('gio', 'null', 'posix'):
		ctx.shlib(
			source = ['julead/backend/%s.c' % (backend,)],
			target = 'julead/backend/%s' % (backend,),
			use = ['GLIB', 'GOBJECT', 'GIO', 'GMODULE', 'julea'],
			includes = ['include'],
			install_path = '${LIBDIR}/julea/backend'
		)

	for tool in ('benchmark', 'config',):
		ctx.program(
			source = ['tools/%s.c' % (tool,)],
			target = 'tools/julea-%s' % (tool,),
			use = ['GLIB', 'GOBJECT', 'GIO', 'julea'],
			includes = ['include'],
			install_path = '${BINDIR}'
		)

	if ctx.env.HAVE_FUSE:
		ctx.program(
			source = ctx.path.ant_glob('fuse/*.c'),
			target = 'fuse/juleafs',
			use = ['GLIB', 'GIO', 'FUSE', 'julea'],
			includes = ['include'],
			install_path = '${BINDIR}'
		)
