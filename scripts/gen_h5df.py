#!/usr/local/bin/python

import os.path
import numpy as np
from pandas import HDFStore,DataFrame

np.random.seed(1234)
x = np.random.rand(5)
y = np.random.randint(0, 1, 5)
z = ["a", "b", "a", "a", "b"]
df = DataFrame({'x':x, 'y':y, 'z':z})

def write_file(format):
    outfile = '../inst/exampledata/pandas_' + format + '.h5'
    
    if os.path.isfile(outfile):
        os.remove(outfile)
    
    hdf = HDFStore(outfile)
    hdf.put('mydata', df, format=format, data_columns=True, encoding="utf-8")
    hdf.close()

write_file('fixed')
write_file('table')
