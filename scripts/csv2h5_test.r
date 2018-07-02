### NOTE this test is somewhat expensive in terms of run time and I/O.

library(hdfio)
library(data.table)



write_multi_file = function(inpath, outfile="/tmp/mf_out.h5")
{
  if (file.exists(outfile))
    file.remove(outfile)
  
  csv2h5(csvdir=inpath, h5out=outfile, dataset="data")
}





write_single_file = function(infile, outfile="/tmp/sf_out.h5")
{
  if (file.exists(outfile))
    file.remove(outfile)
  
  csv2h5(stringsAsFactors=FALSE, csvfile=infile, h5out=outfile, dataset="data")
}

read_single_file = function(incsv, h5file="/tmp/sf_out.h5", stringsAsFactors=FALSE)
{
  x = fread(incsv, stringsAsFactors=stringsAsFactors, data.table=FALSE)
  y = read_h5df(h5file)
  
  stopifnot(identical(dim(x), dim(y)))
  
  all.equal(x, y)
}



# write_multi_file()

csvfile = "~/Downloads/airlines/1987.csv"
write_single_file(csvfile)
read_single_file(csvfile)
