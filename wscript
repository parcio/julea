#!/usr/bin/env python

top = '.'
out = 'build'

def options (ctx):
	ctx.load('compiler_c')

def configure (ctx):
	ctx.load('compiler_c')

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

	ctx.env.CFLAGS += ['-std=c99', '-pedantic', '-Wall', '-Wextra']
	ctx.env.CFLAGS += ['-Wno-missing-field-initializers', '-Wno-unused-parameter', '-Wold-style-definition', '-Wdeclaration-after-statement', '-Wmissing-declarations', '-Wmissing-prototypes', '-Wredundant-decls', '-Wmissing-noreturn', '-Wshadow', '-Wpointer-arith', '-Wcast-align', '-Wwrite-strings', '-Winline', '-Wformat-nonliteral', '-Wformat-security', '-Wswitch-enum', '-Wswitch-default', '-Winit-self', '-Wmissing-include-dirs', '-Wundef', '-Waggregate-return', '-Wmissing-format-attribute', '-Wnested-externs', '-Wstrict-prototypes']

	ctx.env.CFLAGS += ['-D_FILE_OFFSET_BITS=64']

def build (ctx):
	e = ctx.env.derive()
	e.CFLAGS = ['-std=c99']

	ctx.stlib(
		source = ['mongodb/src/%s.c' % file for file in ('bson', 'md5', 'mongo', 'numbers')],
		target = 'mongodb',
		env = e
	)

	# FIXME hack
	e = ctx.env.derive()
	e.CFLAGS = ctx.env.CFLAGS[:]
	e.CFLAGS.remove('-Wshadow')

	ctx.stlib(
		source = ['julea/%s.c' % file for file in ('jbson', 'jbson-iterator', 'jcollection', 'jcollection-iterator', 'jconnection', 'jcredentials', 'jerror', 'jitem', 'jlist', 'jlist-iterator', 'jmongo-connection', 'jobjectid', 'jsemantics', 'jstore', 'jstore-iterator')],
		target = 'julea',
		use = ['GLIB', 'GOBJECT', 'GIO', 'mongodb'],
		includes = ['mongodb/src'],
		env = e
	)

	for test in ('bson', 'bson-iterator', 'list', 'list-iterator', 'semantics'):
		ctx.program(
			source = ['test/%s.c' % (test,)],
			target = 'test/%s' % (test,),
			use = ['GLIB', 'julea'],
			includes = ['julea', 'mongodb/src'],
			env = e
		)

	ctx.program(
		source = ['benchmark.c'],
		target = 'benchmark',
		use = ['GLIB', 'GOBJECT', 'julea'],
		includes = ['julea']
	)
