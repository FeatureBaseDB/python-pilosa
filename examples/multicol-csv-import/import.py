#! /usr/bin/env python3

import sys
import threading

from pilosa import Client, Schema
from pilosa.imports import Column, FieldValue

# adapt these to match the CSV file
INDEX_NAME = "my-index"
INDEX_KEYS = True
FIELDS = [
    {"name": "size", "opts": {"keys": True}},
    {"name": "color", "opts": {"keys": True}},
    {"name": "age", "opts": {"int_min": 0, "int_max": 150}},
]
# -----------------------------

class MultiColumnBitIterator:

    def __init__(self,
            file_obj, field,
            column_index=0, row_index=1,
            has_header=True):
        self.file_obj = file_obj
        if has_header:
            # if there's a header skip it
            next(self.file_obj)

        ci = column_index
        ri = row_index
        
        # set the bit yielder
        if field.field_type == "int":
            if field.index.keys:
                self.yield_fun = lambda fs: FieldValue(column_key=fs[ci], value=int(fs[ri]))
            else:
                self.yield_fun = lambda fs: FieldValue(column_id=int(fs[ci]), value=int(fs[ri]))
        else:
            if field.index.keys:
                if field.keys:
                    self.yield_fun = lambda fs: Column(column_key=fs[ci], row_key=fs[ri] )
                else:
                    self.yield_fun = lambda fs: Column(column_key=fs[ci], row_id=int(fs[ri]))
            else:
                if field.keys:
                    self.yield_fun = lambda fs: Column(column_id=int(fs[ci]), row_key=fs[ri] )
                else:
                    self.yield_fun = lambda fs: Column(column_id=int(fs[ci]), row_id=int(fs[ri]))
    
    def __call__(self):
        yield_fun = self.yield_fun
        for line in self.file_obj:
            # skip empty lines
            line = line.strip()
            if not line:
                continue
            # split fields
            fs = [x.strip() for x in line.split(",")]
            # return a bit
            yield yield_fun(fs)

def import_field(client, field, path, row_index):
    with open(path) as f:
        mcb = MultiColumnBitIterator(f, field, row_index=row_index)
        client.import_field(field, mcb())

def import_csv(pilosa_addr, path):
    client = Client(pilosa_addr, socket_timeout=20000)

    # create the schema
    schema = Schema()
    index = schema.index(INDEX_NAME, keys=INDEX_KEYS, track_existence=True)
    fields = [index.field(field["name"], **field["opts"]) for field in FIELDS]
    client.sync_schema(schema)

    # import each field
    threads = []
    for i, field in enumerate(fields):
        t = threading.Thread(target=import_field, args=(client, field, path, i + 1))
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} pilosa_address csv_file")
        sys.exit(1)
    
    pilosa_addr = sys.argv[1]
    path = sys.argv[2]
    import_csv(pilosa_addr, path)

if __name__ == "__main__":
    main()
