#!/usr/bin/env python

# JULEA - Flexible storage framework
# Copyright (C) 2010-2017 Michael Kuhn
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from waflib import Context, Utils
from waflib.Build import BuildContext

import os
import subprocess

APPNAME = 'julea'
VERSION = '0.2'

top = '.'
out = 'build'

# CentOS 7 has GLib 2.42
glib_version = '2.42'

def options (ctx):
	ctx.load('compiler_c')

	ctx.add_option('--debug', action='store_true', default=False, help='Enable debug mode')
	ctx.add_option('--sanitize', action='store_true', default=False, help='Enable sanitize mode')

	ctx.add_option('--libbson', action='store', default='{0}/external/libbson'.format(Context.run_dir), help='libbson prefix')
	ctx.add_option('--libmongoc', action='store', default='{0}/external/libmongoc'.format(Context.run_dir), help='libmongoc driver prefix')
	ctx.add_option('--otf', action='store', default='{0}/external/otf'.format(Context.run_dir), help='OTF prefix')

	ctx.add_option('--leveldb', action='store', default=None, help='LevelDB prefix')

def configure (ctx):
	ctx.load('compiler_c')
	ctx.load('gnu_dirs')

	ctx.env.CFLAGS += ['-std=c11']
	ctx.env.CFLAGS += ['-fdiagnostics-color']
	ctx.env.CFLAGS += ['-Wpedantic', '-Wall', '-Wextra']
	ctx.define('_POSIX_C_SOURCE', '200809L', quote=False)

	ctx.check_large_file()

	for program in ('mpicc',):
		ctx.find_program(
			program,
			var = program.upper(),
			mandatory = False
		)

	ctx.check_cc(
		lib = 'm',
		uselib_store = 'M'
	)

	for module in ('gio', 'glib', 'gmodule', 'gobject', 'gthread'):
		ctx.check_cfg(
			package = '{0}-2.0'.format(module),
			args = ['--cflags', '--libs', '{0}-2.0 >= {1}'.format(module, glib_version)],
			uselib_store = module.upper()
		)

	ctx.check_cfg(
		package = 'libbson-1.0',
		args = ['--cflags', '--libs', 'libbson-1.0 >= 1.6.0'],
		uselib_store = 'LIBBSON',
		pkg_config_path = '{0}/lib/pkgconfig'.format(ctx.options.libbson)
	)

	ctx.env.JULEA_LIBMONGOC = \
	ctx.check_cfg(
		package = 'libmongoc-1.0',
		args = ['--cflags', '--libs', 'libmongoc-1.0 >= 1.6.0'],
		uselib_store = 'LIBMONGOC',
		pkg_config_path = '{0}/lib/pkgconfig:{1}/lib/pkgconfig'.format(ctx.options.libbson, ctx.options.libmongoc),
		mandatory = False
	)

	ctx.env.JULEA_FUSE = \
	ctx.check_cfg(
		package = 'fuse',
		args = ['--cflags', '--libs'],
		uselib_store = 'FUSE',
		mandatory = False
	)

	if ctx.env.MPICC:
		# MPI
		ctx.env.JULEA_MPI = \
		ctx.check_cc(
			header_name = 'mpi.h',
			lib = Utils.to_list(ctx.cmd_and_log([ctx.env.MPICC, '--showme:libs']).strip()),
			includes = Utils.to_list(ctx.cmd_and_log([ctx.env.MPICC, '--showme:incdirs']).strip()),
			libpath = Utils.to_list(ctx.cmd_and_log([ctx.env.MPICC, '--showme:libdirs']).strip()),
			rpath = Utils.to_list(ctx.cmd_and_log([ctx.env.MPICC, '--showme:libdirs']).strip()),
			uselib_store = 'MPI',
			define_name = 'HAVE_MPI'
		)

	ctx.env.JULEA_LEVELDB = \
	ctx.check_cfg(
		package = 'leveldb',
		args = ['--cflags', '--libs'],
		uselib_store = 'LEVELDB',
		pkg_config_path = '{0}/lib/pkgconfig'.format(ctx.options.leveldb) if ctx.options.leveldb else None,
		mandatory = False
	)

	ctx.check_cc(
		header_name = 'otf.h',
		lib = 'open-trace-format',
		includes = ['{0}/include/open-trace-format'.format(ctx.options.otf)],
		libpath = ['{0}/lib'.format(ctx.options.otf)],
		rpath = ['{0}/lib'.format(ctx.options.otf)],
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

	ctx.check_cc(
		fragment = '''
		#define _POSIX_C_SOURCE 200809L

		#include <stdint.h>

		int main (void)
		{
			uint64_t dummy = 0;

			__sync_fetch_and_add(&dummy, 1);

			return 0;
		}
		''',
		define_name = 'HAVE_SYNC_FETCH_AND_ADD',
		msg = 'Checking for __sync_fetch_and_add',
		mandatory = False
	)

	if ctx.options.sanitize:
		ctx.check_cc(
			cflags = '-fsanitize=address',
			ldflags = '-fsanitize=address',
			uselib_store = 'ASAN',
			mandatory = False
		)

		ctx.check_cc(
			cflags = '-fsanitize=undefined',
			ldflags = '-fsanitize=undefined',
			uselib_store = 'UBSAN',
			mandatory = False
		)

	if ctx.options.debug:
		ctx.env.CFLAGS += ['-Wno-missing-field-initializers', '-Wno-unused-parameter', '-Wold-style-definition', '-Wdeclaration-after-statement', '-Wmissing-declarations', '-Wmissing-prototypes', '-Wredundant-decls', '-Wmissing-noreturn', '-Wshadow', '-Wpointer-arith', '-Wcast-align', '-Wwrite-strings', '-Winline', '-Wformat-nonliteral', '-Wformat-security', '-Wswitch-enum', '-Wswitch-default', '-Winit-self', '-Wmissing-include-dirs', '-Wundef', '-Waggregate-return', '-Wmissing-format-attribute', '-Wnested-externs', '-Wstrict-prototypes']
		ctx.env.CFLAGS += ['-ggdb']

		ctx.define('G_DISABLE_DEPRECATED', 1)
		ctx.define('GLIB_VERSION_MIN_REQUIRED', 'GLIB_VERSION_{0}'.format(glib_version.replace('.', '_')), quote=False)
		ctx.define('GLIB_VERSION_MAX_ALLOWED', 'GLIB_VERSION_{0}'.format(glib_version.replace('.', '_')), quote=False)
	else:
		ctx.env.CFLAGS += ['-O2']

	if ctx.options.debug:
		ctx.define('JULEA_DEBUG', 1)

	if ctx.options.debug:
		# Context.out_dir is empty after the first configure
		out_dir = os.path.abspath(out)
		ctx.define('JULEA_BACKEND_PATH_BUILD', '{0}/backend'.format(out_dir))

	ctx.define('JULEA_BACKEND_PATH', Utils.subst_vars('${LIBDIR}/julea/backend', ctx.env))

	ctx.define('JULEA_COMPILATION', 1)

	ctx.write_config_header('include/julea-config.h')

def build (ctx):
	# Headers
	include_dir = ctx.path.find_dir('include')
	ctx.install_files('${INCLUDEDIR}/julea', include_dir.ant_glob('**/*.h', excl='**/*-internal.h'), cwd=include_dir, relative_trick=True)

	# Trace library
#	ctx.shlib(
#		source = ['lib/jtrace.c'],
#		target = 'lib/jtrace',
#		use = ['GLIB', 'OTF'],
#		includes = ['include'],
#		install_path = '${LIBDIR}'
#	)

	use_julea_core = ['M', 'GLIB', 'ASAN'] # 'UBSAN'
	use_julea_lib = use_julea_core + ['GIO', 'GOBJECT', 'LIBBSON', 'OTF']
	use_julea_backend = use_julea_core + ['GMODULE']

	# Library
	ctx.shlib(
		source = ctx.path.ant_glob('lib/**/*.c'),
		target = 'lib/julea',
		use = use_julea_lib,
		includes = ['include'],
		install_path = '${LIBDIR}'
	)

	# Library (internal)
	ctx.shlib(
		source = ctx.path.ant_glob('lib/**/*.c'),
		target = 'lib/julea-private',
		use = use_julea_lib,
		includes = ['include'],
		defines = ['J_ENABLE_INTERNAL'],
		install_path = '${LIBDIR}'
	)

	clients = ['object', 'kv', 'item']

	for client in clients:
		use_extra = []

		if client == 'item':
			use_extra.append('lib/julea-kv')
			use_extra.append('lib/julea-object')

		ctx.shlib(
			source = ctx.path.ant_glob('client/{0}/**/*.c'.format(client)),
			target = 'lib/julea-{0}'.format(client),
			use = use_julea_lib + ['lib/julea'] + use_extra,
			includes = ['include'],
			defines = ['J_ENABLE_INTERNAL'],
			install_path = '${LIBDIR}'
		)

	# Tests
	ctx.program(
		source = ctx.path.ant_glob('test/**/*.c'),
		target = 'test/julea-test',
		use = use_julea_core + ['lib/julea-private', 'lib/julea-item'],
		includes = ['include', 'test'],
		defines = ['J_ENABLE_INTERNAL'],
		install_path = None
	)

	# Benchmark
	ctx.program(
		source = ctx.path.ant_glob('benchmark/**/*.c'),
		target = 'benchmark/julea-benchmark',
		use = use_julea_core + ['lib/julea-private', 'lib/julea-item'],
		includes = ['include', 'benchmark'],
		defines = ['J_ENABLE_INTERNAL'],
		install_path = None
	)

	# Server
	ctx.program(
		source = ctx.path.ant_glob('server/*.c'),
		target = 'server/julea-server',
		use = use_julea_core + ['lib/julea', 'GIO', 'GMODULE', 'GOBJECT', 'GTHREAD'],
		includes = ['include'],
		defines = ['J_ENABLE_INTERNAL'],
		install_path = '${BINDIR}'
	)

	backends_client = []

	if ctx.env.JULEA_LIBMONGOC:
		backends_client.append('mongodb')

	# Client backends
	for backend in backends_client:
		use_extra = []

		if backend == 'mongodb':
			use_extra = ['LIBMONGOC']

		ctx.shlib(
			source = ['backend/client/{0}.c'.format(backend)],
			target = 'backend/client/{0}'.format(backend),
			use = use_julea_backend + ['lib/julea'] + use_extra,
			includes = ['include'],
			install_path = '${LIBDIR}/julea/backend/client'
		)

	backends_server = ['gio', 'null', 'posix']

	if ctx.env.JULEA_LEVELDB:
		backends_server.append('leveldb')

	# Server backends
	for backend in backends_server:
		use_extra = []

		if backend == 'gio':
			use_extra = ['GIO', 'GOBJECT']
		elif backend == 'leveldb':
			use_extra = ['LEVELDB']

		ctx.shlib(
			source = ['backend/server/{0}.c'.format(backend)],
			target = 'backend/server/{0}'.format(backend),
			use = use_julea_backend + ['lib/julea'] + use_extra,
			includes = ['include'],
			install_path = '${LIBDIR}/julea/backend/server'
		)

	# Command line
	ctx.program(
		source = ctx.path.ant_glob('cli/*.c'),
		target = 'cli/julea-cli',
		use = use_julea_core + ['lib/julea', 'lib/julea-object', 'lib/julea-item'],
		includes = ['include'],
		defines = ['J_ENABLE_INTERNAL'],
		install_path = '${BINDIR}'
	)

	# Tools
	for tool in ('config', 'statistics'):
		ctx.program(
			source = ['tools/{0}.c'.format(tool)],
			target = 'tools/julea-{0}'.format(tool),
			use = use_julea_core + ['lib/julea', 'GIO', 'GOBJECT'],
			includes = ['include'],
			defines = ['J_ENABLE_INTERNAL'],
			install_path = '${BINDIR}'
		)

	# FUSE
	if ctx.env.JULEA_FUSE:
		ctx.program(
			source = ctx.path.ant_glob('fuse/*.c'),
			target = 'fuse/julea-fuse',
			use = use_julea_core + ['lib/julea', 'lib/julea-item', 'FUSE'],
			includes = ['include'],
			install_path = '${BINDIR}'
		)

	# pkg-config
	ctx(
		features = 'subst',
		source = 'pkg-config/julea.pc.in',
		target = 'pkg-config/julea.pc',
		install_path = '${LIBDIR}/pkgconfig',
		APPNAME = APPNAME,
		VERSION = VERSION,
		INCLUDEDIR = Utils.subst_vars('${INCLUDEDIR}', ctx.env),
		LIBDIR = Utils.subst_vars('${LIBDIR}', ctx.env),
		GLIB_VERSION = glib_version
	)
