#' summarize_h5df
#' 
#' TODO
#' 
#' @param h5in
#' Input file.
#' @param dataset
#' TODO
#' 
#' @return
#' TODO
#' 
#' @examples
#' library(hdfio)
#' h5in = system.file("exampledata/pandas_table.h5", package="hdfio")
#' summarize_h5df(h5in)
#' 
#' @export
summarize_h5df = function(h5in, dataset=NULL)
{
  check.is.string(h5in)
  if (!is.null(dataset))
    check.is.string(dataset)
  
  h5_fp = h5file(h5in, mode="r")
  dataset = h5_get_dataset(h5_fp, dataset)
  
  name = h5_fp$get_filename()
  file_size = memuse::Sys.filesize(h5in)
  fmt = h5_detect_format(h5_fp, dataset)
  dim = h5_dim(h5_fp, dataset)
  
  h5close(h5_fp)
  
  ret = list(
    name = name,
    dataset = dataset,
    file_size = file_size,
    format = fmt,
    dim = dim
  )
  
  class(ret) = "summary_h5df"
  ret
}



#' title Print \code{summary_h5df} objects
#' @description Printing for \code{summary_h5df} objects
#' @param x \code{summary_h5df} object
#' @param ... unused
#' @name print-summary_h5df
#' @rdname print-summary_h5df
#' @method print summary_h5df
#' @export
print.summary_h5df = function(x, ...)
{
  cat("Filename:   ", x$name, "\n")
  cat("File size:  ", as.character(x$file_size), "\n")
  cat("Dataset:    ", x$dataset, "\n")
  cat("Format:     ", x$format, "\n")
  if (x$format != "unknown")
    cat("Dimensions: ", x$dim[1], "x", x$dim[2], "\n")
  
  invisible()
}