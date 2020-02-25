#!/usr/bin/env python

# JULEA - Flexible storage framework
# Copyright (C) 2010-2020 Michael Kuhn
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

from waflib import Utils

import os

APPNAME = 'julea'
VERSION = '0.2'

top = '.'
out = 'build'

# Structured logging needs GLib 2.56
# CentOS 7 has GLib 2.42, CentOS 7.6 has GLib 2.56
# Ubuntu 18.04 has GLib 2.56
glib_version = '2.56'
# Ubuntu 18.04 has libfabric 1.5.3
libfabric_version = '1.5.3'
# Ubuntu 18.04 has libbson 1.9.2
libbson_version = '1.9.0'

# Ubuntu 18.04 has LevelDB 1.20
leveldb_version = '1.20'
# Ubuntu 18.04 has LMDB 0.9.21
lmdb_version = '0.9.21'
# Ubuntu 18.04 has libmongoc 1.9.2
libmongoc_version = '1.9.0'
# Ubuntu 18.04 has SQLite 3.22.0
sqlite_version = '3.22.0'
# Ubuntu 18.04 has MariaDB Connector/C 3.0.3
mariadb_version = '3.0.3'


def check_cfg_rpath(ctx, **kwargs):
	r = ctx.check_cfg(**kwargs)

	if ctx.options.debug:
		libpath = 'LIBPATH_{0}'.format(kwargs['uselib_store'])

		if libpath in ctx.env:
			rpath = 'RPATH_{0}'.format(kwargs['uselib_store'])
			ctx.env[rpath] = ctx.env[libpath]

	return r


def check_cc_rpath(ctx, opt, **kwargs):
	if opt in (None, 'no', 'off'):
		return False

	if opt:
		kwargs['includes'] = ['{0}/include'.format(opt)]
		kwargs['libpath'] = ['{0}/lib'.format(opt)]

		if ctx.options.debug:
			kwargs['rpath'] = kwargs['libpath']

	return ctx.check_cc(**kwargs)


def get_rpath(ctx):
	return None
	# return ['{0}/lib'.format(os.path.abspath(ctx.out_dir))]


def check_and_add_flags(ctx, flags, mandatory=True, type=['cflags']):
	if not isinstance(flags, list):
		flags = [flags]

	for flag in flags:
		args = {}

		if 'cflags' in type:
			args['cflags'] = [flag, '-Werror']

		if 'ldflags' in type:
			args['ldflags'] = [flag, '-Werror']

		ret = ctx.check_cc(
			msg='Checking for compiler flag {0}'.format(flag),
			mandatory=mandatory,
			**args
		)

		if ret:
			if 'cflags' in type:
				ctx.env.CFLAGS += [flag]

			if 'ldflags' in type:
				ctx.env.LDFLAGS += [flag]


def get_bin(prefixes, bin):
	env = os.getenv('PATH')

	if env:
		prefixes = ':'.join((env, prefixes))

	for prefix in prefixes.split(':'):
		path = '{0}/bin/{1}'.format(prefix, bin)

		if os.access(path, os.X_OK):
			return path

	return None


def check_and_append_pkg_config_path(path, prefix):
	for component in ('lib', 'lib64', 'share'):
		dir = '{0}/{1}/pkgconfig'.format(prefix, component)

		if os.path.exists(dir):
			path.append(dir)


def get_pkg_config_path(prefix):
	env = os.getenv('PKG_CONFIG_PATH')
	path = []

	# Prefer prefixes that have been set explicitly.
	if prefix:
		check_and_append_pkg_config_path(path, prefix)

	# Use paths from the environment next as they include the dependencies installed via Spack.
	if env:
		path.extend(env.split(':'))

	# If no prefix has been specified, fall back to the global directories.
	if not prefix:
		check_and_append_pkg_config_path(path, '/usr')

	return ':'.join(path)


def options(ctx):
	ctx.load('compiler_c')
	#ctx.load('compiler_cxx')

	ctx.add_option('--debug', action='store_true', default=False, help='Enable debug mode')
	ctx.add_option('--sanitize', action='store_true', default=False, help='Enable sanitize mode')
	ctx.add_option('--coverage', action='store_true', default=False, help='Enable coverage analysis')

	ctx.add_option('--glib', action='store', default=None, help='GLib prefix')
	ctx.add_option('--libfabric', action='store', default=None, help='libfabric prefix')
	ctx.add_option('--leveldb', action='store', default=None, help='LevelDB prefix')
	ctx.add_option('--lmdb', action='store', default=None, help='LMDB prefix')
	ctx.add_option('--libbson', action='store', default=None, help='libbson prefix')
	ctx.add_option('--libmongoc', action='store', default=None, help='libmongoc driver prefix')
	ctx.add_option('--librados', action='store', default=None, help='librados driver prefix')
	ctx.add_option('--hdf5', action='store', default=None, help='HDF5 prefix', dest='hdf')
	ctx.add_option('--otf', action='store', default=None, help='OTF prefix')
	ctx.add_option('--sqlite', action='store', default=None, help='SQLite prefix')
	ctx.add_option('--mariadb', action='store', default=None, help='MariaDB prefix')


def configure(ctx):
	ctx.load('compiler_c')
	#ctx.load('compiler_cxx')
	ctx.load('gnu_dirs')
	ctx.load('clang_compilation_database', tooldir='waf-extras')

	ctx.env.JULEA_DEBUG = ctx.options.debug

	check_and_add_flags(ctx, '-std=c11')
	check_and_add_flags(ctx, '-fdiagnostics-color', False)
	check_and_add_flags(ctx, ['-Wpedantic', '-Wall', '-Wextra'])
	ctx.define('_POSIX_C_SOURCE', '200809L', quote=False)

	ctx.check_large_file()

	ctx.check_cc(
		lib='m',
		uselib_store='M'
	)

	for module in ('gio', 'glib', 'gmodule', 'gobject', 'gthread'):
		check_cfg_rpath(
			ctx,
			package='{0}-2.0'.format(module),
			args=['--cflags', '--libs', '{0}-2.0 >= {1}'.format(module, glib_version)],
			uselib_store=module.upper(),
			pkg_config_path=get_pkg_config_path(ctx.options.glib)
		)

	check_cfg_rpath(
		ctx,
		package='libfabric',
		args=['--cflags', '--libs', 'libfabric >= {0}'.format(libfabric_version)],
		uselib_store='LIBFABRIC',
		pkg_config_path=get_pkg_config_path(ctx.options.libfabric),
		mandatory=False
	)

	check_cfg_rpath(
		ctx,
		package='libbson-1.0',
		args=['--cflags', '--libs', 'libbson-1.0 >= {0}'.format(libbson_version)],
		uselib_store='LIBBSON',
		pkg_config_path=get_pkg_config_path(ctx.options.libbson)
	)

	ctx.env.JULEA_LIBMONGOC = \
		check_cfg_rpath(
			ctx,
			package='libmongoc-1.0',
			args=['--cflags', '--libs', 'libmongoc-1.0 >= {0}'.format(libmongoc_version)],
			uselib_store='LIBMONGOC',
			pkg_config_path=get_pkg_config_path(ctx.options.libmongoc),
			mandatory=False
		)

	# FIXME use check_cfg
	ctx.env.JULEA_LIBRADOS = \
		ctx.check_cc(
			lib='rados',
			uselib_store='LIBRADOS',
			mandatory=False
		)

	ctx.env.JULEA_HDF = \
		check_cc_rpath(
			ctx,
			ctx.options.hdf,
			header_name=['hdf5.h', 'H5PLextern.h'],
			lib='hdf5',
			uselib_store='HDF5',
			define_name='HAVE_HDF5',
			mandatory=False
		)

	ctx.env.JULEA_FUSE = \
		check_cfg_rpath(
			ctx,
			package='fuse',
			args=['--cflags', '--libs'],
			uselib_store='FUSE',
			pkg_config_path=get_pkg_config_path(None),
			mandatory=False
		)

	ctx.env.JULEA_LEVELDB = \
		check_cfg_rpath(
			ctx,
			package='leveldb',
			args=['--cflags', '--libs', 'leveldb >= {0}'.format(leveldb_version)],
			uselib_store='LEVELDB',
			pkg_config_path=get_pkg_config_path(ctx.options.leveldb),
			mandatory=False
		)

	# Ubuntu's package does not ship a pkg-config file
	if not ctx.env.JULEA_LEVELDB:
		ctx.env.JULEA_LEVELDB = \
			check_cc_rpath(
				ctx,
				ctx.options.leveldb,
				header_name='leveldb/c.h',
				lib='leveldb',
				uselib_store='LEVELDB',
				define_name='HAVE_LEVELDB',
				mandatory=False
			)

	ctx.env.JULEA_LMDB = \
		check_cfg_rpath(
			ctx,
			package='lmdb',
			args=['--cflags', '--libs', 'lmdb >= {0}'.format(lmdb_version)],
			uselib_store='LMDB',
			pkg_config_path=get_pkg_config_path(ctx.options.lmdb),
			mandatory=False
		)

	"""
	check_cfg_rpath(
		ctx,
		path = get_bin(ctx.options.otf, 'otfconfig'),
		package = '',
		args = ['--includes', '--libs'],
		uselib_store = 'OTF',
		msg = 'Checking for \'otf\'',
		mandatory = False
	)
	"""

	ctx.env.JULEA_SQLITE = \
		check_cfg_rpath(
			ctx,
			package='sqlite3',
			args=['--cflags', '--libs', 'sqlite3 >= {0}'.format(sqlite_version)],
			uselib_store='SQLITE',
			pkg_config_path=get_pkg_config_path(ctx.options.sqlite),
			mandatory=False
		)

	ctx.env.JULEA_MARIADB = \
		check_cfg_rpath(
			ctx,
			package='libmariadb',
			args=['--cflags', '--libs', 'libmariadb >= {0}'.format(mariadb_version)],
			uselib_store='MARIADB',
			pkg_config_path=get_pkg_config_path(ctx.options.mariadb),
			mandatory=False
		)

	# Ubuntu's package does not ship a pkg-config file
	if not ctx.env.JULEA_MARIADB:
		ctx.env.JULEA_MARIADB = \
			check_cfg_rpath(
				ctx,
				path='mariadb_config',
				package='',
				args=['--cflags', '--libs'],
				uselib_store='MARIADB',
				msg='Checking for mariadb_config',
				mandatory=False
			)

	# stat.st_mtim.tv_nsec
	ctx.check_cc(
		fragment='''
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
		define_name='HAVE_STMTIM_TVNSEC',
		msg='Checking for stat.st_mtim.tv_nsec',
		mandatory=False
	)

	ctx.check_cc(
		fragment='''
		#define _POSIX_C_SOURCE 200809L

		#include <stdint.h>

		int main (void)
		{
			uint64_t dummy = 0;

			__sync_fetch_and_add(&dummy, 1);

			return 0;
		}
		''',
		define_name='HAVE_SYNC_FETCH_AND_ADD',
		msg='Checking for __sync_fetch_and_add',
		mandatory=False
	)

	if ctx.options.sanitize:
		check_and_add_flags(ctx, '-fsanitize=address', False, ['cflags', 'ldflags'])
		# FIXME enable ubsan?
		# check_and_add_flags(ctx, '-fsanitize=undefined', False, ['cflags', 'ldflags'])

	if ctx.options.coverage:
		check_and_add_flags(ctx, '--coverage', True, ['cflags', 'ldflags'])

	if ctx.options.debug:
		check_and_add_flags(ctx, ['-Og', '-g'])

		check_and_add_flags(ctx, [
			'-Waggregate-return',
			'-Wcast-align',
			'-Wcast-qual',
			'-Wdeclaration-after-statement',
			'-Wdouble-promotion',
			'-Wduplicated-cond',
			'-Wfloat-equal',
			'-Wformat=2',
			'-Winit-self',
			'-Winline',
			'-Wjump-misses-init',
			'-Wlogical-op',
			'-Wmissing-declarations',
			'-Wmissing-format-attribute',
			'-Wmissing-include-dirs',
			'-Wmissing-noreturn',
			'-Wmissing-prototypes',
			'-Wnested-externs',
			'-Wnull-dereference',
			'-Wold-style-definition',
			'-Wredundant-decls',
			'-Wrestrict',
			'-Wshadow',
			'-Wstrict-prototypes',
			'-Wswitch-default',
			'-Wswitch-enum',
			'-Wundef',
			'-Wuninitialized',
			'-Wwrite-strings'
		], False)

		ctx.define('G_DISABLE_DEPRECATED', 1)
		ctx.define('GLIB_VERSION_MIN_REQUIRED', 'GLIB_VERSION_{0}'.format(glib_version.replace('.', '_')), quote=False)
		ctx.define('GLIB_VERSION_MAX_ALLOWED', 'GLIB_VERSION_{0}'.format(glib_version.replace('.', '_')), quote=False)

		ctx.define('JULEA_DEBUG', 1)
	else:
		check_and_add_flags(ctx, '-O2')

	ctx.define('G_LOG_USE_STRUCTURED', 1)
	ctx.define('G_LOG_DOMAIN', 'JULEA')

	backend_path = Utils.subst_vars('${LIBDIR}/julea/backend', ctx.env)
	ctx.define('JULEA_BACKEND_PATH', backend_path)

	ctx.write_config_header('include/julea-config.h')


def build(ctx):
	# Headers
	include_dir = ctx.path.find_dir('include')
	include_excl = ['**/*-internal.h']

	if not ctx.env.JULEA_HDF:
		include_excl.append('**/julea-hdf5.h')
		include_excl.append('**/jhdf5.h')
		include_excl.append('**/jhdf5-*.h')

	ctx.install_files('${INCLUDEDIR}/julea', include_dir.ant_glob('**/*.h', excl=include_excl), cwd=include_dir, relative_trick=True)

	use_julea_core = ['M', 'GLIB']
	use_julea_lib = use_julea_core + ['GIO', 'GOBJECT', 'LIBBSON', 'OTF']
	use_julea_backend = use_julea_core + ['GMODULE']
	use_julea_object = use_julea_core + ['lib/julea', 'lib/julea-object']
	use_julea_kv = use_julea_core + ['lib/julea', 'lib/julea-kv']
	use_julea_db = use_julea_core + ['lib/julea', 'lib/julea-db']
	use_julea_item = use_julea_core + ['lib/julea', 'lib/julea-item']
	use_julea_hdf = use_julea_core + ['lib/julea'] + ['lib/julea-hdf5'] if ctx.env.JULEA_HDF else []

	include_julea_core = ['include', 'include/core']

	# Library
	ctx.shlib(
		source=ctx.path.ant_glob('lib/core/**/*.c'),
		target='lib/julea',
		use=use_julea_lib,
		includes=include_julea_core,
		defines=['JULEA_COMPILATION'],
		install_path='${LIBDIR}'
	)

	clients = ['object', 'kv', 'db', 'item']

	if ctx.env.JULEA_HDF:
		clients.append('hdf5')

	for client in clients:
		use_extra = []

		if client == 'item':
			use_extra.append('lib/julea-kv')
			use_extra.append('lib/julea-object')
		elif client == 'hdf5':
			use_extra.append('HDF5')
			use_extra.append('lib/julea-kv')
			use_extra.append('lib/julea-object')

		ctx.shlib(
			source=ctx.path.ant_glob('lib/{0}/**/*.c'.format(client)),
			target='lib/julea-{0}'.format(client),
			use=use_julea_lib + ['lib/julea'] + use_extra,
			includes=include_julea_core,
			defines=['JULEA_{0}_COMPILATION'.format(client.upper())],
			rpath=get_rpath(ctx),
			install_path='${LIBDIR}'
		)

	# Tests
	ctx.program(
		source=ctx.path.ant_glob('test/**/*.c'),
		target='test/julea-test',
		use=use_julea_object + use_julea_kv + use_julea_db + use_julea_item + use_julea_hdf,
		includes=include_julea_core + ['test'],
		rpath=get_rpath(ctx),
		install_path=None
	)

	# Benchmark
	ctx.program(
		source=ctx.path.ant_glob('benchmark/**/*.c'),
		target='benchmark/julea-benchmark',
		use=use_julea_object + use_julea_kv + use_julea_item + use_julea_hdf,
		includes=include_julea_core + ['benchmark'],
		rpath=get_rpath(ctx),
		install_path=None
	)

	# Server
	ctx.program(
		source=ctx.path.ant_glob('server/*.c'),
		target='server/julea-server',
		use=use_julea_core + ['lib/julea', 'GIO', 'GMODULE', 'GOBJECT', 'GTHREAD'],
		includes=include_julea_core,
		rpath=get_rpath(ctx),
		install_path='${BINDIR}'
	)

	object_backends = ['gio', 'null', 'posix']

	if ctx.env.JULEA_LIBRADOS:
		object_backends.append('rados')

	for backend in object_backends:
		use_extra = []

		if backend == 'gio':
			use_extra = ['GIO', 'GOBJECT']
		elif backend == 'rados':
			use_extra = ['LIBRADOS']

		ctx.shlib(
			source=['backend/object/{0}.c'.format(backend)],
			target='backend/object/{0}'.format(backend),
			use=use_julea_backend + ['lib/julea'] + use_extra,
			includes=include_julea_core,
			rpath=get_rpath(ctx),
			install_path='${LIBDIR}/julea/backend/object'
		)

	kv_backends = ['null']

	if ctx.env.JULEA_LEVELDB:
		kv_backends.append('leveldb')

	if ctx.env.JULEA_LMDB:
		kv_backends.append('lmdb')

	if ctx.env.JULEA_LIBMONGOC:
		kv_backends.append('mongodb')

	if ctx.env.JULEA_SQLITE:
		kv_backends.append('sqlite')

	for backend in kv_backends:
		use_extra = []
		cflags = []

		if backend == 'leveldb':
			use_extra = ['LEVELDB']
			# leveldb bug (will be fixed in 1.23)
			# https://github.com/google/leveldb/pull/365
			cflags = ['-Wno-strict-prototypes']
		elif backend == 'lmdb':
			use_extra = ['LMDB']
			# lmdb bug
			cflags = ['-Wno-discarded-qualifiers']
		elif backend == 'mongodb':
			use_extra = ['LIBMONGOC']
		elif backend == 'sqlite':
			use_extra = ['SQLITE']

		ctx.shlib(
			source=['backend/kv/{0}.c'.format(backend)],
			target='backend/kv/{0}'.format(backend),
			use=use_julea_backend + ['lib/julea'] + use_extra,
			includes=include_julea_core,
			cflags=cflags,
			rpath=get_rpath(ctx),
			install_path='${LIBDIR}/julea/backend/kv'
		)

	db_backends = ['null', 'memory']

	if ctx.env.JULEA_SQLITE:
		db_backends.append('sqlite')
	if ctx.env.JULEA_MARIADB:
		db_backends.append('mysql')

	for backend in db_backends:
		use_extra = []
		cflags = []

		if backend == 'sqlite':
			use_extra = ['SQLITE']
		if backend == 'mysql':
			use_extra = ['MARIADB']
			# MariaDB bug
			# https://jira.mariadb.org/browse/CONC-381
			cflags = ['-Wno-strict-prototypes']

		ctx.shlib(
			source=['backend/db/{0}.c'.format(backend)],
			target='backend/db/{0}'.format(backend),
			use=use_julea_backend + ['lib/julea'] + use_extra,
			includes=include_julea_core,
			cflags=cflags,
			rpath=get_rpath(ctx),
			install_path='${LIBDIR}/julea/backend/db'
		)

	# Command line
	ctx.program(
		source=ctx.path.ant_glob('cli/*.c'),
		target='cli/julea-cli',
		use=use_julea_object + use_julea_kv + use_julea_item,
		includes=include_julea_core,
		rpath=get_rpath(ctx),
		install_path='${BINDIR}'
	)

	# Tools
	for tool in ('config', 'statistics'):
		use_extra = []

		if tool == 'statistics':
			use_extra.append('lib/julea')

		ctx.program(
			source=['tools/{0}.c'.format(tool)],
			target='tools/julea-{0}'.format(tool),
			use=use_julea_core + ['GIO', 'GOBJECT'] + use_extra,
			includes=include_julea_core,
			rpath=get_rpath(ctx),
			install_path='${BINDIR}'
		)

	# FUSE
	if ctx.env.JULEA_FUSE:
		ctx.program(
			source=ctx.path.ant_glob('fuse/*.c'),
			target='fuse/julea-fuse',
			use=use_julea_object + use_julea_kv + ['FUSE'],
			includes=include_julea_core,
			rpath=get_rpath(ctx),
			install_path='${BINDIR}'
		)

	# pkg-config
	for lib in ('', 'object', 'kv', 'db', 'item'):
		suffix = '-{0}'.format(lib) if lib else ''

		ctx(
			features='subst',
			source='pkg-config/julea{0}.pc.in'.format(suffix),
			target='pkg-config/julea{0}.pc'.format(suffix),
			install_path='${LIBDIR}/pkgconfig',
			APPNAME=APPNAME,
			VERSION=VERSION,
			INCLUDEDIR=Utils.subst_vars('${INCLUDEDIR}', ctx.env),
			LIBDIR=Utils.subst_vars('${LIBDIR}', ctx.env),
			GLIB_VERSION=glib_version,
			LIBBSON_VERSION=libbson_version
		)

	# Example
	#for extension in ('c', 'cc'):
	#	ctx.program(
	#		source=ctx.path.ant_glob('example/hello-world.{0}'.format(extension)),
	#		target='example/hello-world-{0}'.format(extension),
	#		use=use_julea_object + use_julea_kv + use_julea_db,
	#		includes=include_julea_core,
	#		rpath=get_rpath(ctx),
	#		install_path=None
	#	)
