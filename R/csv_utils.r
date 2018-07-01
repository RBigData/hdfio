csv_nrows = function(csvfile)
{
  lineSampler::wc_l(csvfile)$lines
}

csv_ncols = function(csvfile)
{
  ncol(data.table::fread(csvfile, nrows=0))
}

csv_dim = function(csvfile)
{
  nrows = csv_nrows(csvfile)
  ncols = csv_ncols(csvfile)
  
  c(nrows, ncols)
}



csv_colnames = function(csvfile)
{
  colnames(data.table::fread(csvfile, nrows=0))
}



csv_colstorage = function(csvfile)
{
  # TODO try a sampling strategy
  
  x = data.table::fread(csvfile, nrows=1)
  sapply(x, storage.mode)
}
