#!/usr/bin/env python

top = '.'
out = 'build'

def options (ctx):
	ctx.load('boost')
	ctx.load('compiler_cxx')

def configure (ctx):
	ctx.load('boost')
	ctx.load('compiler_cxx')

	ctx.check_boost(
		lib='filesystem program_options system thread'
	)

#	ctx.check_cfg(
#		package='glibmm-2.4',
#		args='--cflags --libs',
#		uselib_store='GLIBMM',
#		mandatory=True
#	)

	ctx.check_cxx(
		lib='mongoclient'
	)

	ctx.env.CXXFLAGS += ['-Wall', '-Wextra', '-pedantic', '-ggdb']

	ctx.write_config_header('config.h')

def build (ctx):
	ctx.stlib(
		source = ['julea/%s.cc' % file for file in ('collection', 'connection', 'credentials', 'exception', 'item', 'semantics', 'store')],
		target = 'julea',
		use = ['BOOST_FILESYSTEM', 'BOOST_PROGRAM_OPTIONS', 'BOOST_SYSTEM', 'BOOST_THREAD', 'MONGOCLIENT'],
		includes = ['.']
	)

	ctx.program(
		source = ['test.cc'],
		target = 'test',
		use = ['julea'],
		includes = ['.', 'julea']
	)
