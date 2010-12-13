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

	ctx.env.CFLAGS += ['-std=c99', '-pedantic', '-Wall', '-Wextra']
	ctx.env.CFLAGS += ['-Wno-missing-field-initializers', '-Wno-unused-parameter', '-Wold-style-definition', '-Wdeclaration-after-statement', '-Wmissing-declarations', '-Wmissing-prototypes', '-Wredundant-decls', '-Wmissing-noreturn', '-Wshadow', '-Wpointer-arith', '-Wcast-align', '-Wwrite-strings', '-Winline', '-Wformat-nonliteral', '-Wformat-security', '-Wswitch-enum', '-Wswitch-default', '-Winit-self', '-Wmissing-include-dirs', '-Wundef', '-Waggregate-return', '-Wmissing-format-attribute', '-Wnested-externs', '-Wstrict-prototypes']

	ctx.env.CFLAGS += ['-D_FILE_OFFSET_BITS=64']

	ctx.write_config_header('config.h')

def build (ctx):
	ctx.stlib(
		source = ['julea/%s.c' % file for file in ('collection', 'connection', 'credentials', 'exception', 'item', 'semantics', 'store')],
		target = 'julea',
		use = ['GLIB'],
		includes = ['.']
	)

	ctx.program(
		source = ['test.c'],
		target = 'test',
		use = ['GLIB', 'julea'],
		includes = ['.', 'julea']
	)
