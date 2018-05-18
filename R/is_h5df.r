is_h5df_from_pandas = function(h5df_fp, dataset)
{
  !is.null(hdf5r::h5attributes(h5df_fp[[dataset]])$pandas_version)
}



check_is_h5df_from_pandas = function(h5df_fp, dataset)
{
  valid = is_h5df_from_pandas(h5df_fp, dataset)
  if (!valid)
    stop("h5in/dataset does not point to a valid dataset/h5df file")
  
  invisible(TRUE)
}


get_dataset = function(h5df_fp, dataset)
{
  if (is.null(dataset))
  {
    datasets = names(h5df_fp)
    if (length(datasets) != 1)
    {
      hdf5r::h5close(h5df_fp)
      stop("multiple datasets available")
    }
    else
      dataset = datasets[1]
  }
  
  dataset
}



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
is_h5df = function(h5in, dataset=NULL)
{
  check.is.string(h5in)
  if (!is.null(dataset))
    check.is.string(dataset)
  
  f = hdf5r::h5file(h5in, mode="r")
  dataset = get_dataset(f, dataset)
  check = is_h5df_from_pandas(f, dataset)
  hdf5r::h5close(f)
  
  check
}
