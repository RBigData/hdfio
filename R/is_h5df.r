#' is_h5df
#' 
#' Check if an HDF5 dataset is dataframe-like.
#' 
#' @details
#' TODO
#' 
#' @param h5in
#' Input file.
#' @param dataset
#' TODO
#' 
#' @return
#' Returns \code{TRUE} on success, \code{FALSE} otherwise.
#' 
#' @examples
#' library(hdfio)
#' 
#' f = system.file("exampledata/pytables_table.h5", package="hdfio")
#' is_h5df(f, "mydata")
#' is_h5df(f, "/")
#' 
#' f = system.file("exampledata/not_h5df.h5", package="hdfio")
#' is_h5df(f, "mydata")
#' 
#' @seealso
#' \code{\link{is_h5df}}
#' 
#' @export
is_h5df = function(h5in, dataset=NULL)
{
  check.is.string(h5in)
  if (!is.null(dataset))
    check.is.string(dataset)
  
  f = h5file(h5in, mode="r")
  dataset = h5_get_dataset(f, dataset)
  fmt = h5_detect_format(f, dataset, verbose=FALSE)
  h5close(f)
  
  fmt != "unknown"
}
