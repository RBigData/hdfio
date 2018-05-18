#' read_h5df
#' 
#' TODO
#' 
#' @param h5in
#' Input file.
#' @param dataset
#' Dataset in input file to read or \code{NULL}. In the latter case, TODO
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
read_h5df = function(h5in, dataset=NULL)
{
  check.is.string(h5in)
  if (!is.null(dataset))
    check.is.string(dataset)
  
  f = hdf5r::h5file(h5in, mode="r")
  dataset = get_dataset(f, dataset)
  check_is_h5df_from_pandas(f, dataset)
  
  len = f[[dataset]][["table"]]$dims
  df = f[[dataset]][["table"]][1:len]
  df$index = NULL
  
  hdf5r::h5close(f)
  
  df
}
