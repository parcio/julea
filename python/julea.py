from julea_wrapper import lib, ffi

encoding = 'utf-8'

def encode(string):
    result = ffi.new('char[]', string.encode(encoding)) 
    return result

def read_from_buffer(buffer):
    char = ffi.cast('char*', buffer)
    bytearr = b''
    i = 0
    byte = char[i]
    while byte != b'\x00':
        bytearr += byte
        i += 1
        byte = char[i]
    return bytearr.decode()

class JBatchResult:
    IsSuccess = False

    def __init__(self):
        pass

    def Succeed(self):
        self.IsSuccess = True

    def Fail(self):
        self.IsSuccess = False;

class JBatch:
    def __init__(self, result):
        template = lib.J_SEMANTICS_TEMPLATE_DEFAULT
        self.batch = lib.j_batch_new_for_template(template)
        self.result = result

    def __enter__(self):
        return self.batch

    def __exit__(self, exc_type, exc_value, tb):
        if lib.j_batch_execute(self.batch):
            self.result.Succeed()
        else:
            self.result.Fail()
        lib.j_batch_unref(self.batch)
        if exc_type is not None:
            return False
        else:
            return True
