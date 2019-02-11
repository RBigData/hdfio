csv_nrows = function(csvfile)
{
  filesampler::wc_l(csvfile)$lines
}

csv_ncols = function(csvfile)
{
  ncol(csv_reader(csvfile, nrows=0))
}

csv_dim = function(csvfile)
{
  nrows = csv_nrows(csvfile)
  ncols = csv_ncols(csvfile)
  
  c(nrows, ncols)
}



csv_colnames = function(csvfile)
{
  colnames(csv_reader(csvfile, nrows=0))
}



csv_colstorage = function(csvfile)
{
  # TODO try a sampling strategy
  
  x = csv_reader(csvfile, nrows=1)
  sapply(x, storage.mode)
}
