#!/usr/bin/env python

import Utils

top = '.'
out = 'build'

def options (ctx):
	ctx.load('compiler_c')

	ctx.add_option('--debug', action='store_true', default=False, help='Enable debug mode')

	ctx.add_option('--mongodb', action='store', default='/usr', help='Use MongoDB')

	ctx.add_option('--hdtrace', action='store', default=None, help='Use HDTrace')
	ctx.add_option('--otf', action='store', default=None, help='Use OTF')
	ctx.add_option('--zookeeper', action='store', default=None, help='Use ZooKeeper')

def configure (ctx):
	ctx.load('compiler_c')
	ctx.load('gnu_dirs')

	ctx.env.CFLAGS += ['-std=c99']

	#ctx.check_large_file()

	ctx.check_cfg(
		package = 'gio-2.0',
		args = ['--cflags', '--libs'],
		atleast_version = '2.30',
		uselib_store = 'GIO'
	)

	ctx.check_cfg(
		package = 'glib-2.0',
		args = ['--cflags', '--libs'],
		atleast_version = '2.30',
		uselib_store = 'GLIB'
	)

	ctx.check_cfg(
		package = 'gmodule-2.0',
		args = ['--cflags', '--libs'],
		atleast_version = '2.30',
		uselib_store = 'GMODULE'
	)

	ctx.check_cfg(
		package = 'gobject-2.0',
		args = ['--cflags', '--libs'],
		atleast_version = '2.30',
		uselib_store = 'GOBJECT'
	)

	ctx.check_cfg(
		package = 'gthread-2.0',
		args = ['--cflags', '--libs'],
		atleast_version = '2.30',
		uselib_store = 'GTHREAD'
	)

	ctx.check_cfg(
		package = 'fuse',
		args = ['--cflags', '--libs'],
		uselib_store = 'FUSE',
		mandatory = False
	)

	# BSON
	ctx.check_cc(
		header_name = 'bson.h',
		lib = 'bson',
		includes = ['%s/src' % (ctx.options.mongodb,)],
		libpath = ['%s' % (ctx.options.mongodb,)],
		rpath = ['%s' % (ctx.options.mongodb,)],
		uselib_store = 'BSON',
		define_name = 'HAVE_BSON'
	)

	# MongoDB
	ctx.check_cc(
		header_name = 'mongo.h',
		lib = 'mongoc',
		includes = ['%s/src' % (ctx.options.mongodb,)],
		libpath = ['%s' % (ctx.options.mongodb,)],
		rpath = ['%s' % (ctx.options.mongodb,)],
		uselib_store = 'MONGODB',
		define_name = 'HAVE_MONGODB'
	)

	if ctx.options.hdtrace:
		ctx.check_cc(
			header_name = 'hdTrace.h',
			lib = 'hdTracing',
			includes = ['%s/include' % (ctx.options.hdtrace,)],
			libpath = ['%s/lib' % (ctx.options.hdtrace,)],
			rpath = ['%s/lib' % (ctx.options.hdtrace,)],
			uselib_store = 'HDTRACE',
			define_name = 'HAVE_HDTRACE'
		)

	if ctx.options.otf:
		ctx.check_cc(
			header_name = 'otf.h',
			lib = 'otf',
			includes = ['%s/include' % (ctx.options.otf,)],
			libpath = ['%s/lib' % (ctx.options.otf,)],
			rpath = ['%s/lib' % (ctx.options.otf,)],
			uselib_store = 'OTF',
			define_name = 'HAVE_OTF'
		)

	if ctx.options.zookeeper:
		ctx.check_cc(
			header_name = 'zookeeper.h',
			lib = 'zookeeper_mt',
			includes = ['%s/include/c-client-src' % (ctx.options.zookeeper,)],
			libpath = ['%s/lib' % (ctx.options.zookeeper,)],
			rpath = ['%s/lib' % (ctx.options.zookeeper,)],
			uselib_store = 'ZOOKEEPER',
			define_name = 'HAVE_ZOOKEEPER'
		)

	if ctx.options.debug:
		ctx.env.CFLAGS += ['-pedantic', '-Wall', '-Wextra']
		ctx.env.CFLAGS += ['-Wno-missing-field-initializers', '-Wno-unused-parameter', '-Wold-style-definition', '-Wdeclaration-after-statement', '-Wmissing-declarations', '-Wmissing-prototypes', '-Wredundant-decls', '-Wmissing-noreturn', '-Wshadow', '-Wpointer-arith', '-Wcast-align', '-Wwrite-strings', '-Winline', '-Wformat-nonliteral', '-Wformat-security', '-Wswitch-enum', '-Wswitch-default', '-Winit-self', '-Wmissing-include-dirs', '-Wundef', '-Waggregate-return', '-Wmissing-format-attribute', '-Wnested-externs', '-Wstrict-prototypes']
		ctx.env.CFLAGS += ['-ggdb']

		ctx.define('G_DISABLE_DEPRECATED', 1)
	else:
		ctx.env.CFLAGS += ['-O2']

def build (ctx):
	# Headers
	ctx.install_files('${INCLUDEDIR}/julea', ctx.path.ant_glob('include/*.h', excl = 'include/*-internal.h'))

	# Trace library
#	ctx.shlib(
#		source = ['lib/jtrace.c'],
#		target = 'lib/jtrace',
#		use = ['GLIB', 'HDTRACE', 'OTF'],
#		includes = ['include'],
#		install_path = '${LIBDIR}'
#	)

	# Library
	ctx.shlib(
		source = ctx.path.ant_glob('lib/*.c'),
		target = 'lib/julea',
		use = ['GIO', 'GLIB', 'GOBJECT', 'BSON', 'MONGODB', 'HDTRACE', 'OTF'],
		includes = ['include'],
		install_path = '${LIBDIR}'
	)

	# Tests
	ctx.program(
		source = ctx.path.ant_glob('test/*.c') + ctx.path.ant_glob('lib/*.c'),
		target = 'test/test',
		use = ['GIO', 'GLIB', 'GOBJECT', 'BSON', 'MONGODB', 'HDTRACE', 'OTF'],
		includes = ['include'],
		install_path = None
	)

	# Daemon
	ctx.program(
		source = ['julead/%s.c' % file for file in ('julead',)],
		target = 'julead/julead',
		use = ['lib/julea', 'GIO', 'GLIB', 'GMODULE', 'GOBJECT', 'GTHREAD'],
		includes = ['include'],
		defines = ['JULEAD_BACKEND_PATH="%s"' % (Utils.subst_vars('${LIBDIR}/julea/backend', ctx.env),)],
		install_path = '${BINDIR}'
	)

	# Daemon backends
	for backend in ('gio', 'null', 'posix'):
		ctx.shlib(
			source = ['julead/backend/%s.c' % (backend,)],
			target = 'julead/backend/%s' % (backend,),
			use = ['lib/julea', 'GIO', 'GLIB', 'GMODULE', 'GOBJECT'],
			includes = ['include'],
			install_path = '${LIBDIR}/julea/backend'
		)

	# Command line
	ctx.program(
		source = ctx.path.ant_glob('cmd/*.c'),
		target = 'cmd/julea',
		use = ['lib/julea', 'GIO', 'GLIB', 'GOBJECT'],
		includes = ['include'],
		install_path = '${BINDIR}'
	)

	# Tools
	for tool in ('benchmark', 'config'):
		ctx.program(
			source = ['tools/%s.c' % (tool,)],
			target = 'tools/julea-%s' % (tool,),
			use = ['lib/julea', 'GIO', 'GLIB', 'GOBJECT'],
			includes = ['include'],
			install_path = '${BINDIR}'
		)

	# FUSE
	if ctx.env.HAVE_FUSE:
		ctx.program(
			source = ctx.path.ant_glob('fuse/*.c'),
			target = 'fuse/juleafs',
			use = ['lib/julea', 'GLIB', 'GOBJECT', 'FUSE'],
			includes = ['include'],
			install_path = '${BINDIR}'
		)

	# pkg-config
	ctx(
		features = 'subst',
		source = 'pkg-config/julea.pc.in',
		target = 'pkg-config/julea.pc',
		install_path = '${LIBDIR}/pkgconfig',
		INCLUDEDIR = Utils.subst_vars('${INCLUDEDIR}', ctx.env),
		LIBDIR = Utils.subst_vars('${LIBDIR}', ctx.env)
	)
