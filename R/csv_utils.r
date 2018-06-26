csv_getdim = function(csvfile)
{
  nrows = lineSampler::wc_l(csvfile)$lines
  ncols = ncol(data.table::fread(csvfile, nrows=0))
  
  c(nrows, ncols)
}



csv_getcolnames = function(csvfile)
{
  colnames(data.table::fread(csvfile, nrows=0))
}



csv_getstorage = function(csvfile)
{
  # TODO try a sampling strategy
  
  x = data.table::fread(csvfile, nrows=1)
  sapply(x, storage.mode)
}
