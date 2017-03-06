#!/usr/bin/env python

from waflib import Context, Utils
from waflib.Build import BuildContext

import os
import subprocess

top = '.'
out = 'build'

# CentOS 7 has GLib 2.42
glib_version = '2.42'

class BenchmarkContext (BuildContext):
	cmd = 'benchmark'
	fun = 'benchmark'

class EnvironmentContext (BuildContext):
	cmd = 'environment'
	fun = 'environment'

class TestContext (BuildContext):
	cmd = 'test'
	fun = 'test'

def get_glib_options ():
	return 'G_SLICE=all'

def get_path ():
	return 'PATH={0}/server:{0}/tools:{1}/external/mongo-c-driver/bin:${{PATH}}'.format(Context.out_dir, Context.run_dir)

def get_library_path ():
	return 'LD_LIBRARY_PATH={0}/lib:{1}/external/mongo-c-driver/lib:${{LD_LIBRARY_PATH}}'.format(Context.out_dir, Context.run_dir)

def get_pkg_config_path ():
	return 'PKG_CONFIG_PATH={0}/pkg-config:{1}/external/mongo-c-driver/lib:${{PKG_CONFIG_PATH}}'.format(Context.out_dir, Context.run_dir)

def options (ctx):
	ctx.load('compiler_c')

	ctx.add_option('--debug', action='store_true', default=False, help='Enable debug mode')
	ctx.add_option('--sanitize', action='store_true', default=False, help='Enable sanitize mode')

	ctx.add_option('--mongodb', action='store', default='{0}/external/mongo-c-driver'.format(Context.run_dir), help='MongoDB driver prefix')
	ctx.add_option('--otf', action='store', default='{0}/external/otf'.format(Context.run_dir), help='OTF prefix')

	ctx.add_option('--jzfs', action='store', default=None, help='JZFS prefix')

	ctx.add_option('--leveldb', action='store', default='/usr', help='Use LevelDB')

def benchmark (ctx):
	setup = '{0}/tools/setup.sh'.format(Context.top_dir)
	command = '{0} {1}/benchmark/benchmark'.format(get_library_path(), Context.out_dir)

	subprocess.call('{0} start'.format(setup), close_fds=True, shell=True)
	subprocess.call(command, close_fds=True, shell=True)
	subprocess.call('{0} stop'.format(setup), close_fds=True, shell=True)

def test (ctx):
	setup = '{0} {1}/tools/setup.sh'.format(get_glib_options(), Context.top_dir)
	gtester = Utils.subst_vars('${GTESTER}', ctx.env)
	command = '{0} {1} {2} {3} --keep-going --verbose {4}/test/test'.format(get_glib_options(), get_library_path(), 'gdb --args ' if ctx.options.debug else '', gtester, Context.out_dir)

	subprocess.call('{0} start'.format(setup), close_fds=True, shell=True)
	subprocess.call(command, close_fds=True, shell=True)
	subprocess.call('{0} stop'.format(setup), close_fds=True, shell=True)

def environment (ctx):
	path = get_path()
	ld_library_path = get_library_path()
	pkg_config_path = get_pkg_config_path()

	with open('julea-environment', 'w') as f:
		f.write(path + '\n')
		f.write(ld_library_path + '\n')
		f.write(pkg_config_path + '\n')
		f.write('\n')
		f.write('export PATH\n')
		f.write('export LD_LIBRARY_PATH\n')
		f.write('export PKG_CONFIG_PATH\n')

def configure (ctx):
	ctx.load('compiler_c')
	ctx.load('gnu_dirs')

	ctx.env.CFLAGS += ['-std=c99']
	ctx.env.CFLAGS += ['-fdiagnostics-color']
	ctx.env.CFLAGS += ['-Wpedantic', '-Wall', '-Wextra']
	ctx.define('_POSIX_C_SOURCE', '200809L', quote=False)

	ctx.check_large_file()

	for program in ('gtester', 'mpicc'):
		ctx.find_program(
			program,
			var = program.upper(),
			mandatory = False
		)

	for module in ('gio', 'glib', 'gmodule', 'gobject', 'gthread'):
		ctx.check_cfg(
			package = '{0}-2.0'.format(module),
			args = ['--cflags', '--libs', '{0}-2.0 >= {1}'.format(module, glib_version)],
			uselib_store = module.upper()
		)

	for module in ('bson', 'mongoc'):
		ctx.check_cfg(
			package = 'lib{0}-1.0'.format(module),
			args = ['--cflags', '--libs'],
			uselib_store = module.upper(),
			pkg_config_path = '{0}/lib/pkgconfig'.format(ctx.options.mongodb)
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

	if ctx.options.jzfs:
		# JZFS
		ctx.env.JULEA_JZFS = \
		ctx.check_cc(
			header_name = 'jzfs.h',
			lib = 'jzfs',
			use = ['GLIB'],
			includes = ['{0}/include/jzfs'.format(ctx.options.jzfs)],
			libpath = ['{0}/lib'.format(ctx.options.jzfs)],
			rpath = ['{0}/lib'.format(ctx.options.jzfs)],
			uselib_store = 'JZFS',
			define_name = 'HAVE_JZFS'
		)

	ctx.env.JULEA_LEVELDB = \
	ctx.check_cc(
		header_name = 'leveldb/c.h',
		lib = 'leveldb',
		includes = ['{0}/include'.format(ctx.options.leveldb)],
		libpath = ['{0}/lib'.format(ctx.options.leveldb)],
		rpath = ['{0}/lib'.format(ctx.options.leveldb)],
		uselib_store = 'LEVELDB',
		define_name = 'HAVE_LEVELDB',
		mandatory = False
	)

	ctx.env.JULEA_LEXOS = \
	ctx.check_cfg(
		package = 'lexos',
		args = ['--cflags', '--libs'],
		uselib_store = 'LEXOS',
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
		# Context.out_dir is empty after the first configure
		out_dir = os.path.abspath(out)
		ctx.define('JULEA_BACKEND_PATH_BUILD', '{0}/backend'.format(out_dir))

	ctx.define('JULEA_BACKEND_PATH', Utils.subst_vars('${LIBDIR}/julea/backend', ctx.env))

	ctx.write_config_header('include/julea-config.h')

def build (ctx):
	# Headers
	ctx.install_files('${INCLUDEDIR}/julea', ctx.path.ant_glob('include/*.h', excl='include/*-internal.h'))

	# Trace library
#	ctx.shlib(
#		source = ['lib/jtrace.c'],
#		target = 'lib/jtrace',
#		use = ['GLIB', 'OTF'],
#		includes = ['include'],
#		install_path = '${LIBDIR}'
#	)

	use_julea_core = ['GLIB', 'ASAN', 'UBSAN']
	use_julea_lib = use_julea_core + ['GIO', 'GOBJECT', 'BSON', 'OTF']
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

	# Tests
	ctx.program(
		source = ctx.path.ant_glob('test/*.c'),
		target = 'test/test',
		use = use_julea_core + ['lib/julea-private'],
		includes = ['include'],
		defines = ['J_ENABLE_INTERNAL'],
		install_path = None
	)

	# Benchmark
	ctx.program(
		source = ctx.path.ant_glob('benchmark/*.c'),
		target = 'benchmark/benchmark',
		use = use_julea_core + ['lib/julea-private'],
		includes = ['include'],
		defines = ['J_ENABLE_INTERNAL'],
		install_path = None
	)

	# Server
	ctx.program(
		source = ctx.path.ant_glob('server/*.c'),
		target = 'server/julea-server',
		use = use_julea_core + ['lib/julea-private', 'GIO', 'GMODULE', 'GOBJECT', 'GTHREAD'],
		includes = ['include'],
		defines = ['J_ENABLE_INTERNAL'],
		install_path = '${BINDIR}'
	)

	# Client backends
	for backend in ('mongodb',):
		use_extra = []

		if backend == 'mongodb':
			use_extra = ['MONGOC']

		ctx.shlib(
			source = ['backend/client/{0}.c'.format(backend)],
			target = 'backend/client/{0}'.format(backend),
			use = use_julea_backend + ['lib/julea'] + use_extra,
			includes = ['include'],
			install_path = '${LIBDIR}/julea/backend/client'
		)

	backends_server = ['gio', 'null', 'posix']

	if ctx.env.JULEA_JZFS and ctx.env.JULEA_LEVELDB:
		backends_server.append('jzfs')

	if ctx.env.JULEA_LEXOS and ctx.env.JULEA_LEVELDB:
		backends_server.append('lexos')

	# Server backends
	for backend in backends_server:
		use_extra = []

		if backend == 'gio':
			use_extra = ['GIO', 'GOBJECT']
		elif backend == 'jzfs':
			use_extra = ['JZFS', 'LEVELDB']
		elif backend == 'lexos':
			use_extra = ['LEXOS', 'LEVELDB']

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
		use = use_julea_core + ['lib/julea-private'],
		includes = ['include'],
		defines = ['J_ENABLE_INTERNAL'],
		install_path = '${BINDIR}'
	)

	# Tools
	for tool in ('config', 'statistics'):
		ctx.program(
			source = ['tools/{0}.c'.format(tool)],
			target = 'tools/julea-{0}'.format(tool),
			use = use_julea_core + ['lib/julea-private', 'GIO', 'GOBJECT'],
			includes = ['include'],
			defines = ['J_ENABLE_INTERNAL'],
			install_path = '${BINDIR}'
		)

	# FUSE
	if ctx.env.JULEA_FUSE:
		ctx.program(
			source = ctx.path.ant_glob('fuse/*.c'),
			target = 'fuse/julea-fuse',
			use = use_julea_core + ['lib/julea', 'FUSE'],
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
