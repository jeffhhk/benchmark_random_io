#!/usr/bin/env python
# coding: utf-8

import sys
from multiprocessing.dummy import Pool as ThreadPool
import time
import os
import random
import io
from timeit import timeit
import math
import json

# *** Parameters that might need per-machine adjustment.  Will be copied to the output result.
num_mb=100*1024              # File size in MB.  Should be >= 2X the physical RAM of the machine to mitigate OS file cache effects.
work_per_configuration=30    # Each configuration is sent the same total amount of work, divided among some number of threads
                             # this parameter scales that amount of work.


mb=1024*1024

"""
Create an empty file of zeros at afile.

Note that there are filesystems that are smart enough to game a benchmark based on file zeros.
E.g. ZFS, the "Zettabyte file system" on Solaris has had sparse file support for at least 20 years.

    https://en.wikipedia.org/wiki/Sparse_file

The policy for sparsifying "holes" into files is nonportable!

When writing this script, I was surprised to learn that ext4 in fact now has support for representing
sparse files.

I developed this script running ext4 on Ubuntu 20.  According to "How to identify sparse files"
on this page, I am not getting accidentally sparsified on that system:

    https://www.ctrl.blog/entry/sparse-files.html

See also the table labelled "Which file systems support sparse files".
"""
def ensure_empty_file(afile):
    if os.path.exists(afile):
        return
    if not os.path.exists(os.path.dirname(afile)):
        raise Exception("cannot write to directory: {}".format(os.path.dirname(afile)))
    cmd = "dd if=/dev/zero of={} bs={} count={}".format(afile, mb, num_mb)
    print("about to {}".format(cmd))
    os.system(cmd)

def random_read(afile, diMax, cIter):
    rand=random.Random()
    with open(afile, "rb") as f:
        for i in range(cIter):
            offs=rand.randint(0,diMax)
            f.seek(offs, io.SEEK_SET)
            f.read(1)
    return 0

def timeit1(fn):
    return timeit(fn, number=1)

def inparallel(c, fn):
    with ThreadPool(processes=c) as pool:
        pool.map(fn, range(c))

def random_read_inparallel(afile1, num_threads, num_io):
    print("about to run {} reads in each of {} threads".format(num_io, num_threads))
    dt = timeit1(lambda: inparallel(num_threads, lambda junk: random_read(afile1, num_mb*mb, num_io)))
    print("finished {} threads in {} seconds".format(num_threads, dt))
    return num_threads*num_io/dt

def random_read_rough_estimate_1_thread(afile):
    return int(random_read_inparallel(afile,1,10000))

def main():
    afile1=sys.argv[1]
    ensure_empty_file(afile1)

    num_reads_per_sec_rough=random_read_rough_estimate_1_thread(afile1)

    thread_counts=[math.floor(math.exp(math.log(40)*c/10)) for c in range(1,11)]

    num_reads_by_num_threads=[(c,random_read_inparallel(afile1, c, int(work_per_configuration*num_reads_per_sec_rough/c))) for c in thread_counts]
    print(json.dumps({
        "num_mb": num_mb,
        "work_per_configuration": work_per_configuration,
        "num_reads_per_sec_rough": num_reads_per_sec_rough,
        "num_reads_by_num_threads": num_reads_by_num_threads
    }))

main()