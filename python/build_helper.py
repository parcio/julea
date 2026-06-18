from os import popen, system
from os.path import dirname
import cffi

def create_header(filename, libraries):
    content = """typedef int gint;
typedef unsigned int guint;
typedef gint gboolean;
typedef char gchar;

typedef unsigned short guint16;
typedef signed int gint32;
typedef unsigned int guint32;
typedef signed long gint64;
typedef unsigned long guint64;

typedef void* gpointer;
typedef const void *gconstpointer;

typedef unsigned long gsize;

typedef guint32 GQuark;

typedef struct _GError GError;
struct _GError
{
    GQuark  domain;
    gint    code;
    gchar  *message;
};

typedef    struct _GModule GModule;

typedef struct _GInputStream GInputStream;
typedef struct _GOutputStream GOutputStream;

typedef struct _GKeyFile GKeyFile;

typedef struct _GSocketConnection GSocketConnection;

typedef void (*GDestroyNotify) (gpointer data);

typedef struct _bson_t
{
    uint32_t    flags;
    uint32_t    len;
    uint8_t     padding[120];
} bson_t;

typedef struct JBatch JBatch;
extern "Python" void cffi_j_kv_get_function(gpointer, guint32, gpointer);
extern "Python" void cffi_j_batch_async_callback(JBatch*, gboolean, gpointer);

"""
    for library in libraries:
        content+=f"#include <{library}.h>\n"
    with open(filename, "w") as file:
        file.write(content)

def get_additional_compiler_flags(libraries, remove_sanitize=True):
    flags_buffer = popen(f"pkg-config --cflags {' '.join(libraries)}")
    flags = flags_buffer.read().strip().split(' ')
    # remove duplicate parameters
    flags = [*set(flags)]
    if remove_sanitize:
        for s in flags:
            if "-fsanitize" in s:
                flags.remove(s)
    return flags

def get_include_dirs(flags):
    return [ str.strip("-I") for str in flags if "-I" in str ]

def collect_julea(filename, libraries, debug = False):
    temp_filename = "temp.h"
    create_header(temp_filename, libraries)
    includes = get_additional_compiler_flags(libraries)
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
    # list of macros to be ignored
    macros = [
            "-D'G_DEFINE_AUTOPTR_CLEANUP_FUNC(x, y)='",
            "-D'G_END_DECLS='",
            "-D'G_BEGIN_DECLS='",
            "-D'G_GNUC_WARN_UNUSED_RESULT='",
            "-D'G_GNUC_PRINTF(x, y)='"
            ]
    # let preprocessor collect all declarations
    system(f"gcc -E -P {' '.join(macros)} {temp_filename} -I. {' '.join(flags)} -o {filename}")
    # remove temporary files needed to please the preprocessor
    system(f"rm -rf glib.h gmodule.h bson.h gio {temp_filename}")

def process(libs, tempheader, debug=False):
    ffi = cffi.FFI()
    libraryname = "julea_wrapper"
    with open(tempheader, "r") as file:
        header_content = file.read()
    includes = get_additional_compiler_flags(libs+["glib-2.0"], remove_sanitize=True)
    include_dirs = get_include_dirs(includes)
    ffi.cdef(header_content, override=True)
    outdir = f"{dirname(__file__)}/../bld/"
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

def copy_main_module():
    system(f"cp {dirname(__file__)}/julea.py {dirname(__file__)}/../bld/julea.py")

def build(include_libs, debug=False):
    header_name = "header_julea.h"
    collect_julea(header_name, include_libs, debug)
    process(include_libs, header_name, debug)
