#' read_h5df
#' 
#' TODO
#' 
#' @param h5in
#' Input file.
#' @param dataset
#' Dataset in input file to read.
#' 
#' @return
#' A dataframe.
#' 
#' @examples
#' library(hdfio)
#' 
#' h5in = system.file("exampledata/from_pandas.h5", package="hdfio")
#' df = read_h5df(h5in, "mydata")
#' df
#' 
#' @seealso
#' \code{\link{is_h5df}}
#' 
#' @export
read_h5df = function(h5in, dataset)
{
  valid = is_h5df(h5in=h5in, dataset=dataset)
  if (!valid)
    stop("h5in/dataset does not point to a valid dataset/h5df file")
  
  f = h5file(h5in)
  
  len = f[[dataset]][["table"]]$dims
  df = f[[dataset]][["table"]][1:len]
  df$index = NULL
  
  hdf5r::h5close(f)
  
  df
}
