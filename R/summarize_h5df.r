read_coltypes = function(h5_fp, dataset)
{
  colnames = h5attributes(h5_fp[[dataset]])$VARNAMES
  ncols = length(colnames)
  
  t = character(length(colnames))
  for (j in 1:ncols)
  {
    nm = paste0("x", j)
    class = h5attributes(h5_fp[[glue(dataset, nm)]])$CLASS
    
    if (length(class) == 0)
    {
      h5t = h5_fp[[glue(dataset, nm)]]$get_type()$to_text()
      if (grepl("H5T_IEEE_F64", h5t))
        t[j] = "double"
      else if (grepl("H5T_IEEE_F32", h5t))
        t[j] = "float"
      else if (grepl("H5T_STD_I32", h5t))
        t[j] = "integer"
      else
        t[j] = "unknown"
    }
    else if (class == H5_STORAGE_STR)
      t[j] = "character"
    else if (class == H5_STORAGE_LGL)
      t[j] = "logical"
    else if (class == H5_STORAGE_DATE)
      t[j] = "date"
    else if (class == H5_STORAGE_FAC)
      t[j] = "factor"
    else
      close_and_stop(h5_fp, INTERNAL_ERROR)
  }
  
  t
}



summarize_dataset = function(h5_fp, dataset, colnames)
{
  format = h5_detect_format(h5_fp, dataset)
  if (format == "unknown")
    return(list(dataset=dataset, format=format, dim=NULL, colnames=NULL))
  
  dim = h5_dim(h5_fp, dataset)
  
  if (isTRUE(colnames))
  {
    cn = h5_colnames(h5_fp, dataset)
    ct = read_coltypes(h5_fp, dataset)
  }
  else
  {
    cn = NULL
    ct = NULL
  }
  
  list(dataset=dataset, format=format, dim=dim, colnames=cn, coltypes=ct)
}



#' summarize_h5df
#' 
#' TODO
#' 
#' @param h5in
#' Input file.
#' @param dataset
#' Name of the data within the HDF5 file. If none is supplied, then this will be
#' inferred from the input file name.
#' @param colnames
#' TODO
#' 
#' @return
#' TODO
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
      ncols = length(ds$colnames)
      ncols_nd = ndigits(ncols)
      colnames_nc = max(nchar(ds$colnames)) + 1
      
      cat(
        "        Columns:    ",
        "\n",
        paste0("           ",
        sprintf(paste0("%", ncols_nd, "d"), 1:ncols),
        ". ",
        sprintf(paste0("%-", colnames_nc, "s"), ds$colnames),
        ds$coltypes,
        "\n")
      )
    }
  }
  
  invisible()
}
