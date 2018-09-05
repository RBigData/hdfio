summarize_dataset = function(h5_fp, dataset, colnames)
{
  format = h5_detect_format(h5_fp, dataset)
  if (format == "unknown")
    return(list(dataset=dataset, format=format, dim=NULL, colnames=NULL))
  
  dim = h5_dim(h5_fp, dataset)
  
  if (isTRUE(colnames))
    cn = h5_colnames(h5_fp, dataset)
  else
    cn = NULL
  
  list(dataset=dataset, format=format, dim=dim, colnames=cn)
}



#' summarize_h5df
#' 
#' TODO
#' 
#' @param h5in
#' Input file.
#' @param dataset
#' TODO
#' @param colnames
#' TODO
#' 
#' @return
#' TODO
#' 
#' @examples
#' library(hdfio)
#' f = system.file("exampledata/pytables_table.h5", package="hdfio")
#' summarize_h5df(f)
#' 
#' @export
summarize_h5df = function(h5in, dataset=NULL, colnames=FALSE)
{
  check.is.string(h5in)
  if (!is.null(dataset))
    check.is.string(dataset)
  check.is.flag(colnames)
  
  h5_fp = h5file(h5in, mode="r")
  
  if (is.null(dataset))
  {
    datasets = h5_list_datasets(h5_fp, dataset)
    datasets_summary = lapply(datasets, summarize_dataset, h5_fp=h5_fp, colnames=colnames)
  }
  else
    datasets_summary = list(summarize_dataset(h5_fp, dataset, colnames))
  
  name = h5_fp$get_filename()
  file_size = memuse::Sys.filesize(h5in)
  
  h5close(h5_fp)
  
  ret = list(
    name = name,
    dataset = dataset,
    file_size = file_size,
    datasets_summary = datasets_summary
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
  cat("Datasets:\n")
  
  for (ds in x$datasets_summary)
  {
    cat("   ", ds$dataset, "\n")
    cat("        Format:    ", ds$format, "\n")
    if (ds$format != "unknown")
      cat("        Dimensions:", ds$dim[1], "x", ds$dim[2], "\n")
    if (!is.null(ds$colnames))
    {
      num = 1:length(ds$colnames)
      cat("        Columns:    ", "\n", paste0("           ", num, ". ", ds$colnames, "\n"))
    }
  }
  
  invisible()
}
