from os import popen, system
from os.path import dirname
import cffi
import json
import os
import itertools

def create_header(filename, libraries, typedefs):
    content = ""
    for (k, v) in typedefs.items():
        content += f"typedef {v} {k};\n"
        if 'struct' in v:
            content += f"{v} {{ ...; }};\n"
    for library in libraries:
        content+=f"#include <{library}.h>\n"
    with open(filename, "w") as file:
        file.write(content)

def get_additional_compiler_flags(libraries, build_dir):
    flags = list(set(itertools.chain.from_iterable(map(
        lambda x: x["target_sources"][0]["parameters"],
        [x for x in json.load(os.popen(f"meson introspect --targets {build_dir}")) if x["name"] in libraries]))))
    clean = []
    for s in flags:
        if "-W" in s:
            pass
        elif "-fsanitize" in s:
            pass
        else:
            clean.append(s)
    return clean

def get_include_dirs(flags):
    return [ str.strip("-I") for str in flags if "-I" in str ]

def collect_julea(filename, libraries, build_dir, typedefs, macros, debug = False):
    temp_filename = "temp.h"
    create_header(temp_filename, libraries, typedefs)
    includes = get_additional_compiler_flags(libraries, build_dir)
    flags = list(filter(lambda entry: not "dependencies" in entry, includes))
    # create dummy headers for files intentionally not included
    with open("glib.h", "w") as file:
        file.write("")
    with open("gmodule.h", "w") as file:
        file.write("")
    with open("bson.h", "w") as file:
        file.write("")
    system("mkdir -p gio")
    with open("gio/gio.h", "w") as file:
        file.write("")
    macro_flags = map(lambda x: f"-D'{x}='", macros)
    # let preprocessor collect all declarations
    system(f"cc -E -P {' '.join(macro_flags)} {temp_filename} -I. {' '.join(flags)} -o {filename}")
    # remove temporary files needed to please the preprocessor
    system(f"rm -rf glib.h gmodule.h bson.h gio {temp_filename}")

def process(build_dir, libs, tempheader, debug=False):
    ffi = cffi.FFI()
    libraryname = "julea_wrapper"
    with open(tempheader, "r") as file:
        header_content = file.read()
    includes = get_additional_compiler_flags(libs+["glib-2.0"], build_dir)
    include_dirs = get_include_dirs(includes)
    try:
        ffi.cdef(header_content, override=True)
    except cffi.CDefError as err:
        print(err)
    
    outdir = build_dir
    headerincludes = ""
    for lib in libs:
        headerincludes += f'#include "{lib}.h"\n'
    ffi.set_source(
            libraryname,
            headerincludes,
            libraries=libs+["kv-null"],
            include_dirs=include_dirs,
            library_dirs=[outdir],
            extra_compile_args=includes,
            extra_link_args=["-Wl,-rpath,."]
            )
    ffi.compile(tmpdir=outdir, verbose=debug)
    if not debug:
        system(f"rm -f {tempheader} {outdir+libraryname}.o {outdir+libraryname}.c")

def copy_main_module(build_dir):
    system(f"cp {dirname(__file__)}/julea.py {build_dir}/julea.py")

def build(build_dir, include_libs, typedefs, macros, debug=False):
    header_name = "header_julea.h"
    collect_julea(header_name, include_libs, build_dir, typedefs, macros, debug)
    process(build_dir, include_libs, header_name, debug)
