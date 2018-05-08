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
#' f = system.file("exampledata/from_pandas.h5", package="hdfio")
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
is_h5df = function(h5in, dataset)
{
  if (missing(h5in))
    stop("mu")
  if (missing(dataset))
    stop("must")
  
  check.is.string(h5in)
  check.is.string(dataset)
  
  
  f = hdf5r::h5file(h5in, mode="r")
  check = !is.null(hdf5r::h5attributes(f[[dataset]])$pandas_version)
  hdf5r::h5close(f)
  
  check
}
