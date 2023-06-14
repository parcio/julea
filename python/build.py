from build_helper import build, copy_main_module
import sys
import os

# types used in JULEA include headers
# use '...' for types which are not accessed in header files
typedefs = {
    # basic glib types
    'gint': 'int',
    'guint': 'unsigned int',
    'gboolean': 'int',
    'gchar': 'char',
    'gdouble': 'double',
    'gfloat': 'float',
    'guint16': 'unsigned short',
    'gint32': 'signed int',
    'guint32': 'unsigned int',
    'gint64': 'signed long',
    'guint64': 'unsigned long',
    'gpointer': 'void*',
    'gconstpointer': 'const void*',
    'gsize': 'unsigned long',

    # glib types
    'GQuark': 'guint32',
    'GError': 'struct _GError', # struct used in static value
    'GModule': '...',
    'GInputStream': '...',
    'GOutputStream': '...',
    'GKeyFile': '...',
    'GSocketConnection': '...',
    'void (*GDestroyNotify) (gpointer data)': '', # function definition

    'bson_t': 'struct _bson_t', # sturct used in static value
    
    'JBatch': '...',
}

# list of macros to be ignored
macros = [
    'G_DEFINE_AUTOPTR_CLEANUP_FUNC(x, y)',
    'G_END_DECLS',
    'G_BEGIN_DECLS',
    'G_GNUC_WARN_UNUSED_RESULT',
    'G_GNUC_PRINTF(x, y)'
]

# JULEA modules which available through the interface
# it is expected that 'include/<module>.h' is a header file!
libraries = [
    "julea",
    "julea-object",
    "julea-kv",
    "julea-db",
    "julea-item"
]

# callbacks to python functions to use julea callback functions
callback_functions = [
    "void cffi_j_kv_get_function(gpointer, guint32, gpointer)",
    "void cffi_j_batch_async_callback(JBatch*, gboolean, gpointer)",
]

def usage():
    print("Build python bindings for JULEA with cffi.")
    print()
    print(f"python {sys.argv[0]} <build-directory>")
    print()
    print("Make sure to execute it from the JULEA-repo root directory!")
    exit(1)

def err(msg):
    print(msg)
    exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage()
    build_dir = os.path.abspath(sys.argv[1])
    if not os.path.isdir(build_dir):
        err(f"Build directory: '{build_dir}' does not exist!")
    if not os.path.isfile(os.path.abspath('.') + "/meson.build"):
        err("The execution directory is apperntly not the JULEA-repo root directory")
    print(build_dir)
    build(build_dir, libraries, typedefs, macros, callback_functions, debug=True)
    copy_main_module(build_dir)
