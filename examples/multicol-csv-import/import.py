#! /usr/bin/env python3

import sys
import threading
from queue import Queue

from pilosa import Client, Schema
from pilosa.imports import Column, FieldValue

# adapt these to match the CSV file
INDEX_NAME = "my-index"
INDEX_KEYS = True
FIELDS = [
    {"name": "size", "opts": {"keys": True}},
    {"name": "color", "opts": {"keys": True}},
    {"name": "age", "opts": {
        "int_min": 0,
        "int_max": 150
    }},
    {"name": "result", "opts": {
        "float_min": 1.13106317,
        "float_max": 30.23959735,
        "float_frac": 8, # number of fractional digits
    }}
]
# -----------------------------
# other settings
THREAD_COUNT = 0  # 0 = use the number of CPUs available to this process
VERBOSE = True
#------------------------------

if not THREAD_COUNT:
    import os
    THREAD_COUNT = len(os.sched_getaffinity(0))


class MultiColumnBitIterator:

    def __init__(self,
            file_obj, field,
            column_index=0, row_index=1,
            has_header=True,
            float_frac=0):
        self.file_obj = file_obj
        if has_header:
            # if there's a header skip it
            next(self.file_obj)

        ci = column_index
        ri = row_index
        float_mul = 10**float_frac

        def row_value(fs):
            try:
                if float_frac:
                    # try to get row id field as a float
                    return int(float(fs[ri]) * float_mul)
                else:
                    # try to getrow id field as an int
                    return int(fs[ri])
            except ValueError:
                # cannot convert to a float or int, skip this one
                return None

        def field_with_column_key(fs):
            value = row_value(fs)
            if value is None:
                return None
            return FieldValue(column_key=fs[ci], value=value)

        def field_with_column_id(fs):
            value = row_value(fs)
            if value is None:
                return None
            return FieldValue(column_id=int(fs[ci]), value=value)

        # set the bit yielder
        if field.field_type == "int":
            if field.index.keys:
                self.yield_fun = field_with_column_key
            else:
                self.yield_fun = field_with_column_id
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
            bit = yield_fun(fs)
            if bit is not None:
                yield bit


def import_field(q, client, path):
    while True:
        item = q.get()
        if item is None:
            break
        field, row_index, float_frac = item
        print("Importing field:", field.name)
        with open(path) as f:
            mcb = MultiColumnBitIterator(f,
                                         field,
                                         row_index=row_index,
                                         float_frac=float_frac)
            client.import_field(field, mcb())
        q.task_done()


def import_csv(pilosa_addr, path):
    client = Client(pilosa_addr, socket_timeout=20000000)

    # create the schema
    schema = Schema()
    index = schema.index(INDEX_NAME, keys=INDEX_KEYS, track_existence=True)
    fields = []
    for field in FIELDS:
        opts = field.get("opts", {})

        # check whether opts include float related fields
        # and convert them to int fields
        float_frac = 0
        if "float_frac" in opts:
            float_frac = opts["float_frac"]
            del opts["float_frac"]
        if "float_min" in opts:
            opts["int_min"] = int(opts["float_min"] * 10**float_frac)
            del opts["float_min"]
        if "float_max" in opts:
            opts["int_max"] = int(opts["float_max"] * 10**float_frac)
            del opts["float_max"]

        field = index.field(field["name"], **opts)
        fields.append((field, float_frac))

    client.sync_schema(schema)

    # import each field
    q = Queue()
    threads = []
    for i in range(THREAD_COUNT):
        t = threading.Thread(target=import_field,
                             args=(q, client, path))
        t.start()
        threads.append(t)

    for i, (field, float_frac) in enumerate(fields):
        q.put((field, i + 1, float_frac))

    # wait for imports to finish
    q.join()

    # stop workers
    for _ in threads:
        q.put(None)
    for t in threads:
        t.join()


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} pilosa_address csv_file")
        sys.exit(1)
    
    pilosa_addr = sys.argv[1]
    path = sys.argv[2]

    print("Pilosa Address:", pilosa_addr)
    print("Thread Count  :", THREAD_COUNT)
    print("CSV Path      :", path)
    print("Verbose       :", VERBOSE)
    print()

    import_csv(pilosa_addr, path)

if __name__ == "__main__":
    main()
