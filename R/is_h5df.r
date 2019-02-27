# is_h5df
#' 
#' Check if an HDF5 dataset is dataframe-like.
#' 
#' @details
#' TODO
#' 
#' @param h5in
#' Input file.
#' @param dataset
#' Name of the data within the HDF5 file. If none is supplied, then this will be
#' inferred from the input file name.
#' 
#' @return
#' Returns \code{TRUE} on success, \code{FALSE} otherwise.
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
