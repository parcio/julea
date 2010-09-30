#!/usr/bin/env python

import os

import Utils

def set_options (opt):
	opt.tool_options('boost')
	opt.tool_options('compiler_cxx')

def configure (conf):
	conf.check_tool('boost')
	conf.check_tool('compiler_cxx')

	conf.check_boost(
		lib='filesystem program_options system thread',
		mandatory=True
	)

#	conf.check_cfg(
#		package='glibmm-2.4',
#		args='--cflags --libs',
#		uselib_store='GLIBMM',
#		mandatory=True
#	)

	conf.check_cxx(
		lib='mongoclient',
		mandatory=True
	)

	conf.env.CXXFLAGS += ['-Wall', '-pedantic']

	conf.write_config_header('config.h')

def build (bld):
	bld.new_task_gen(
		features = 'cxx cstaticlib',
		source = ['julea/%s.cc' % file for file in ('collection', 'credentials', 'exception', 'item', 'semantics', 'store')],
		target = 'julea',
		uselib = ['BOOST_FILESYSTEM', 'BOOST_PROGRAM_OPTIONS', 'BOOST_THREAD', 'MONGOCLIENT'],
		includes = ['.']
	)

	bld.new_task_gen(
		features = 'cxx cprogram',
		source = ['test.cc'],
		target = 'test',
		uselib = ['BOOST_FILESYSTEM', 'BOOST_PROGRAM_OPTIONS', 'BOOST_THREAD', 'MONGOCLIENT'],
		uselib_local = ['julea'],
		includes = ['.', 'julea']
	)
