from build_helper import build, copy_main_module

if __name__ == "__main__":
    build(["julea", "julea-object", "julea-kv", "julea-db", "julea-item"])
    copy_main_module()
