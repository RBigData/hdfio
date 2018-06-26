HDFIO_VERSION = "1.0-0"

write_numeric_column = function(x, h5_fp, dataset, varname)
{
  h5_fp[[dataset]][[varname]] = x
}



write_logical_column = write_numeric_column



write_factor_column = function(x, h5_fp, dataset, varname)
{
  if (is.character(x))
    x = factor(x)
  
  levels = levels(x)
  x.int = as.integer(x)
  
  h5_fp[[dataset]][[varname]] = x.int
  h5attr(h5_fp[[dataset]][[varname]], "LEVELS") = levels
  h5attr(h5_fp[[dataset]][[varname]], "CLASS") = "factor"
}



write_string_column = function(x, h5_fp, dataset, varname)
{
  len = hdfio:::get_max_str_len(x)
  str_fixed_len <- H5T_STRING$new(size = len)
  
  dims = H5S$new(dims=length(x), maxdims=Inf)
  h5_fp[[dataset]]$create_dataset(name=varname, dtype=str_fixed_len, space=dims)
  
  h5_fp[[dataset]][[varname]][] = x
}



write_h5df_column = function(x, h5_fp, dataset, stringsAsFactors=TRUE)
{
  h5attr(h5_fp, "TABLE_FORMAT") = "hdfio_column"
  h5attr(h5_fp, "HDFIO_VERSION") = HDFIO_VERSION
  
  if (!existsGroup(h5_fp, dataset))
    createGroup(h5_fp, dataset)
  
  h5attr(h5_fp[[dataset]], "VARNAMES") = names(x)
  
  for (j in 1:ncol(x))
  {
    nm = paste0("x", j)
    col = x[, j]
    if (class(col) == "numeric" || class(col) == "integer")
      write_numeric_column(col, h5_fp, dataset, nm)
    else if (class(col) == "logical")
      write_logical_column(col, h5_fp, dataset, nm)
    else if ((class(col) == "character" && isTRUE(stringsAsFactors)) || class(col) == "factor")
      write_factor_column(col, h5_fp, dataset, nm)
    else if (class(col) == "character")
      write_string_column(col, h5_fp, dataset, nm)
    else
      stop("")
  }
}



#' write_h5df
#' 
#' TODO
#' 
#' @param x
#' Input dataset (dataframe or datatable)
#' @param file
#' Output file.
#' @param dataset
#' Dataset in input file to read or \code{NULL}. In the latter case, TODO
#' @param stringsAsFactors
#' TODO
#' @param format
#' TODO
#' 
#' @return
#' A dataframe.
#' 
#' @examples
#' library(hdfio)
#' 
#' # TODO
#' 
#' @seealso
#' \code{\link{read_h5df}}
#' 
#' @export
write_h5df = function(x, file, dataset, stringsAsFactors=TRUE, format="column")
{
  if (data.table::is.data.table(x))
    data.table::setDF(x)
  else if (!is.data.frame(x))
    stop("argument 'x' must be a data.frame or data.table object")
  
  check.is.string(file)
  check.is.string(dataset)
  check.is.flag(stringsAsFactors)
  check.is.string(format)
  
  format = match.arg(tolower(format), c("column")) # TODO compound
  
  h5_fp = h5file(file, mode="a")
  
  if (format == "column")
    write_h5df_column(x, h5_fp, dataset, stringsAsFactors)
  
  h5close(h5_fp)
}
