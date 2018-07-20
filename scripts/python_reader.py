import h5py
import pandas
import numpy as np

def read_h5df(file, dataset=None, rows=None, cols=None):
  f = h5py.File(file)
  
  if not 'HDFIO_VERSION' in f.attrs.keys():
    f.close()
    raise ValueError('File seemingly not written by hdfio')
  
  if dataset == None:
    keys = f.keys()
    if len(keys) == 0:
      f.close()
      raise ValueError('No datasets in file')
    elif len(keys) > 1:
      f.close()
      raise ValueError('Multiple datasets in file; please choose one')
    else:
      dataset = keys[0]
  
  m = f[dataset + '/x1'].shape[0]
  colnames = f[dataset].attrs.values()[0]
  n = colnames.shape[0]
  
  if  rows == None:
    rows = range(0, m)
  if cols == None:
    cols = range(1, n+1)
  
  l = list()
  for i in cols:
    tmp = f[dataset + '/x' + str(i)][rows]
    l.append(tmp)
  
  f.close()
  
  df = pandas.DataFrame(l).T
  df.columns = colnames
  return df


read_h5df("/tmp/asdf.h5", rows=range(0, 3))
read_h5df("/tmp/not.h5")
read_h5df("/tmp/not.h5", dataset="df2")
