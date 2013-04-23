#!/usr/bin/env python

from waflib import Context, Utils

import os

top = '.'
out = 'build'

def options (ctx):
	ctx.load('compiler_c')

	ctx.add_option('--debug', action='store_true', default=False, help='Enable debug mode')

	ctx.add_option('--mongodb', action='store', default='%s/external/mongodb-client' % (Context.run_dir,), help='MongoDB client prefix')
	ctx.add_option('--otf', action='store', default='%s/external/otf' % (Context.run_dir,), help='OTF prefix')
	ctx.add_option('--openmpi', action='store', default='%s/external/openmpi' % (Context.run_dir,), help='OpenMPI prefix')

	ctx.add_option('--jzfs', action='store', default=None, help='JZFS prefix')

	ctx.add_option('--leveldb', action='store', default='/usr', help='Use LevelDB')
	ctx.add_option('--hdtrace', action='store', default=None, help='Use HDTrace')

def configure (ctx):
	ctx.load('compiler_c')
	ctx.load('gnu_dirs')

	ctx.env.CFLAGS += ['-std=c99']

	#ctx.check_large_file()

	ctx.find_program(
		'mpicc',
		var = 'MPICC',
		path_list = ['%s/bin' % (ctx.options.openmpi,)],
		mandatory = False
	)

	ctx.check_cfg(
		package = 'gio-2.0',
		args = ['--cflags', '--libs'],
		atleast_version = '2.32',
		uselib_store = 'GIO'
	)

	ctx.check_cfg(
		package = 'glib-2.0',
		args = ['--cflags', '--libs'],
		atleast_version = '2.32',
		uselib_store = 'GLIB'
	)

	ctx.check_cfg(
		package = 'gmodule-2.0',
		args = ['--cflags', '--libs'],
		atleast_version = '2.32',
		uselib_store = 'GMODULE'
	)

	ctx.check_cfg(
		package = 'gobject-2.0',
		args = ['--cflags', '--libs'],
		atleast_version = '2.32',
		uselib_store = 'GOBJECT'
	)

	ctx.check_cfg(
		package = 'gthread-2.0',
		args = ['--cflags', '--libs'],
		atleast_version = '2.32',
		uselib_store = 'GTHREAD'
	)

	ctx.env.JULEA_FUSE = \
	ctx.check_cfg(
		package = 'fuse',
		args = ['--cflags', '--libs'],
		uselib_store = 'FUSE',
		mandatory = False
	)

	if ctx.env.MPICC:
		mpi_env = ctx.env.derive()

		mpi_env.CC = ctx.env.MPICC
		mpi_env.LINK_CC = ctx.env.MPICC
		mpi_env.CC_NAME = os.path.basename(mpi_env.CC)
		mpi_env.COMPILER_CC = os.path.basename(mpi_env.CC)

		# MPI
		ctx.check_cc(
			header_name = 'mpi.h',
			lib = Utils.to_list(ctx.cmd_and_log([ctx.env.MPICC, '--showme:libs']).strip()),
			includes = Utils.to_list(ctx.cmd_and_log([ctx.env.MPICC, '--showme:incdirs']).strip()),
			libpath = Utils.to_list(ctx.cmd_and_log([ctx.env.MPICC, '--showme:libdirs']).strip()),
			rpath = Utils.to_list(ctx.cmd_and_log([ctx.env.MPICC, '--showme:libdirs']).strip()),
			uselib_store = 'MPI',
			define_name = 'HAVE_MPI',
			env = mpi_env
		)

	# BSON
	ctx.check_cc(
		header_name = 'bson.h',
		lib = 'bson',
		includes = ['%s/include' % (ctx.options.mongodb,)],
		libpath = ['%s/lib' % (ctx.options.mongodb,)],
		rpath = ['%s/lib' % (ctx.options.mongodb,)],
		uselib_store = 'BSON',
		define_name = 'HAVE_BSON'
	)

	# MongoDB
	ctx.check_cc(
		header_name = 'mongo.h',
		lib = 'mongoc',
		includes = ['%s/include' % (ctx.options.mongodb,)],
		libpath = ['%s/lib' % (ctx.options.mongodb,)],
		rpath = ['%s/lib' % (ctx.options.mongodb,)],
		uselib_store = 'MONGODB',
		define_name = 'HAVE_MONGODB'
	)

	if ctx.options.jzfs:
		# JZFS
		ctx.env.JULEA_JZFS = \
		ctx.check_cc(
			header_name = 'jzfs.h',
			lib = 'jzfs',
			use = ['GLIB'],
			includes = ['%s/include/jzfs' % (ctx.options.jzfs,)],
			libpath = ['%s/lib' % (ctx.options.jzfs,)],
			rpath = ['%s/lib' % (ctx.options.jzfs,)],
			uselib_store = 'JZFS',
			define_name = 'HAVE_JZFS'
		)

		ctx.check_cc(
			header_name = 'leveldb/c.h',
			lib = 'leveldb',
			includes = ['%s/include' % (ctx.options.leveldb,)],
			libpath = ['%s/lib' % (ctx.options.leveldb,)],
			rpath = ['%s/lib' % (ctx.options.leveldb,)],
			uselib_store = 'LEVELDB',
			define_name = 'HAVE_LEVELDB'
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

	ctx.check_cc(
		header_name = 'otf.h',
		lib = 'otf',
		includes = ['%s/include' % (ctx.options.otf,)],
		libpath = ['%s/lib' % (ctx.options.otf,)],
		rpath = ['%s/lib' % (ctx.options.otf,)],
		uselib_store = 'OTF',
		define_name = 'HAVE_OTF',
		mandatory = False
	)

	# stat.st_mtim.tv_nsec
	ctx.check_cc(
		fragment = '''
		#define _POSIX_C_SOURCE 200809L

		#include <sys/types.h>
		#include <sys/stat.h>
		#include <unistd.h>

		int main (void)
		{
			struct stat stbuf;

			(void)stbuf.st_mtim.tv_nsec;

			return 0;
		}
		''',
		define_name = 'HAVE_STMTIM_TVNSEC',
		msg = 'Checking for stat.st_mtim.tv_nsec',
		mandatory = False
	)

	if ctx.options.debug:
		ctx.env.CFLAGS += ['-pedantic', '-Wall', '-Wextra']
		ctx.env.CFLAGS += ['-Wno-missing-field-initializers', '-Wno-unused-parameter', '-Wold-style-definition', '-Wdeclaration-after-statement', '-Wmissing-declarations', '-Wmissing-prototypes', '-Wredundant-decls', '-Wmissing-noreturn', '-Wshadow', '-Wpointer-arith', '-Wcast-align', '-Wwrite-strings', '-Winline', '-Wformat-nonliteral', '-Wformat-security', '-Wswitch-enum', '-Wswitch-default', '-Winit-self', '-Wmissing-include-dirs', '-Wundef', '-Waggregate-return', '-Wmissing-format-attribute', '-Wnested-externs', '-Wstrict-prototypes']
		ctx.env.CFLAGS += ['-ggdb']

		ctx.define('G_DISABLE_DEPRECATED', 1)
	else:
		ctx.env.CFLAGS += ['-O2']

	ctx.define('DAEMON_BACKEND_PATH', Utils.subst_vars('${LIBDIR}/julea/backend', ctx.env))

	ctx.write_config_header('include/julea-config.h')

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

	# Library (internal)
	ctx.shlib(
		source = ctx.path.ant_glob('lib/*.c'),
		target = 'lib/julea-private',
		use = ['GIO', 'GLIB', 'GOBJECT', 'BSON', 'MONGODB', 'HDTRACE', 'OTF'],
		includes = ['include'],
		defines = ['J_ENABLE_INTERNAL'],
		install_path = '${LIBDIR}'
	)

	# Tests
	ctx.program(
		source = ctx.path.ant_glob('test/*.c'),
		target = 'test/test',
		use = ['lib/julea-private', 'GLIB'],
		includes = ['include'],
		defines = ['J_ENABLE_INTERNAL'],
		install_path = None
	)

	# Benchmark
	ctx.program(
		source = ctx.path.ant_glob('benchmark/*.c'),
		target = 'benchmark/benchmark',
		use = ['lib/julea-private', 'GLIB'],
		includes = ['include'],
		defines = ['J_ENABLE_INTERNAL'],
		install_path = None
	)

	for benchmark in ('mongodb', 'small-access'):
		# Benchmarks
		ctx.program(
			source = 'benchmarks/%s.c' % (benchmark,),
			target = 'benchmarks/%s' % (benchmark,),
			use = ['lib/julea-private', 'GIO', 'GLIB', 'GOBJECT', 'BSON', 'MONGODB', 'MPI'],
			includes = ['include'],
			defines = ['J_ENABLE_INTERNAL'],
			install_path = None
		)

	# Daemon
	ctx.program(
		source = ctx.path.ant_glob('daemon/*.c'),
		target = 'daemon/julea-daemon',
		use = ['lib/julea-private', 'GIO', 'GLIB', 'GMODULE', 'GOBJECT', 'GTHREAD'],
		includes = ['include'],
		defines = ['J_ENABLE_INTERNAL'],
		install_path = '${BINDIR}'
	)

	# Daemon backends
	for backend in ('gio', 'null', 'posix'):
		ctx.shlib(
			source = ['daemon/backend/%s.c' % (backend,)],
			target = 'daemon/backend/%s' % (backend,),
			use = ['lib/julea', 'GIO', 'GLIB', 'GMODULE', 'GOBJECT'],
			includes = ['include'],
			install_path = '${LIBDIR}/julea/backend'
		)

	if ctx.env.JULEA_JZFS:
		ctx.shlib(
			source = ['daemon/backend/jzfs.c'],
			target = 'daemon/backend/jzfs',
			use = ['lib/julea', 'GIO', 'GLIB', 'GMODULE', 'GOBJECT', 'JZFS', 'LEVELDB'],
			includes = ['include'],
			install_path = '${LIBDIR}/julea/backend'
		)

	# Command line
	ctx.program(
		source = ctx.path.ant_glob('cli/*.c'),
		target = 'cli/julea-cli',
		use = ['lib/julea-private', 'GLIB'],
		includes = ['include'],
		defines = ['J_ENABLE_INTERNAL'],
		install_path = '${BINDIR}'
	)

	# Tools
	for tool in ('config', 'statistics'):
		ctx.program(
			source = ['tools/%s.c' % (tool,)],
			target = 'tools/julea-%s' % (tool,),
			use = ['lib/julea-private', 'GIO', 'GLIB', 'GOBJECT'],
			includes = ['include'],
			defines = ['J_ENABLE_INTERNAL'],
			install_path = '${BINDIR}'
		)

	# FUSE
	if ctx.env.JULEA_FUSE:
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
