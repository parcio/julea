from build_helper import build, copy_main_module
import sys
import os

if __name__ == "__main__":
    build_dir = os.path.abspath(sys.argv[1])
    print(build_dir)
    build(build_dir, ["julea", "julea-object", "julea-kv", "julea-db", "julea-item"])
    copy_main_module(build_dir)
