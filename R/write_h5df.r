check_df_cols = function(df)
{
  for (j in 1:NCOL(df))
  {
    type = class(df[, j])
    if (type != "numeric" && type != "integer" && type != "logical" && type != "character" && type != "factor")
      stop("dataframe includes non-atomic/non-factor type")
  }
}



write_numeric_column = function(x, start_ind, h5_fp, dataset, varname)
{
  if (start_ind == 1)
  {
    if (typeof(x) == "double")
      dtype = h5types$H5T_NATIVE_DOUBLE
    else # int and logical
      dtype = h5types$H5T_NATIVE_INT
    
    dims = H5S$new(dims=length(x), maxdims=Inf)
    h5_fp[[dataset]]$create_dataset(name=varname, dtype=dtype, space=dims)
  }
  
  h5_fp[[glue(dataset, varname)]][start_ind : (start_ind+length(x)-1)] = x
}



write_logical_column = function(x, start_ind, h5_fp, dataset, varname)
{
  write_numeric_column(x, start_ind, h5_fp, dataset, varname)
  
  if (start_ind == 1)
    h5attr(h5_fp[[glue(dataset, varname)]], "CLASS") = "logical"
}



write_factor_column = function(x, start_ind, h5_fp, dataset, varname)
{
  if (is.character(x))
    x = factor(x)
  
  x.int = as.integer(x)
  
  if (start_ind == 1)
  {
    dtype = h5types$H5T_NATIVE_INT
    dims = H5S$new(dims=length(x), maxdims=Inf)
    h5_fp[[dataset]]$create_dataset(name=varname, dtype=dtype, space=dims)
    
    # TODO merge factor levels on successive writes
    levels = levels(x)
    h5attr(h5_fp[[glue(dataset, varname)]], "LEVELS") = levels
    h5attr(h5_fp[[glue(dataset, varname)]], "CLASS") = "factor"
  }
  
  h5_fp[[glue(dataset, varname)]][start_ind : (start_ind+length(x)-1)] = x.int
}



write_string_column = function(x, start_ind, h5_fp, dataset, varname)
{
  # TODO check str len
  if (start_ind == 1)
  {
    len = get_max_str_len(x)
    str_fixed_len <- H5T_STRING$new(size = len)
    
    dims = H5S$new(dims=length(x), maxdims=Inf)
    h5_fp[[dataset]]$create_dataset(name=varname, dtype=str_fixed_len, space=dims)
  }
  
  h5_fp[[glue(dataset, varname)]][start_ind : (start_ind+length(x)-1)] = x
}



write_h5df_column = function(x, start_ind, h5_fp, dataset, strlens)
{
  if (start_ind == 1)
  {
    h5attr(h5_fp, "TABLE_FORMAT") = "hdfio_column"
    h5attr(h5_fp, "HDFIO_VERSION") = HDFIO_VERSION
    
    if (!existsGroup(h5_fp, dataset))
    {
      createGroup(h5_fp, dataset)
      h5attr(h5_fp[[dataset]], "VARNAMES") = names(x)
    }
  }
  
  
  for (j in 1:ncol(x))
  {
    nm = paste0("x", j)
    col = x[, j]
    if (class(col) == "numeric" || class(col) == "integer")
      write_numeric_column(col, start_ind, h5_fp, dataset, nm)
    else if (class(col) == "logical")
      write_logical_column(col, start_ind, h5_fp, dataset, nm)
    else if (class(col) == "factor")
      write_factor_column(col, start_ind, h5_fp, dataset, nm)
    else if (class(col) == "character")
      write_string_column(col, start_ind, h5_fp, dataset, nm)
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
#' @param format
#' TODO
#' @param compression
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
write_h5df = function(x, file, dataset, format="column", compression=0)
{
  if (data.table::is.data.table(x))
    data.table::setDF(x)
  else if (!is.data.frame(x))
    stop("argument 'x' must be a data.frame or data.table object")
  
  check_df_cols(x)
  
  check.is.string(file)
  check.is.string(dataset)
  check.is.string(format)
  check.is.natnum(compression)
  if (compression > 9)
    stop("argument 'compression' must be an integer from 0 to 9 (inclusive)")
  
  format = match.arg(tolower(format), c("column")) # TODO compound
  
  h5_fp = h5file(file, mode="a")
  h5_check_dataset(h5_fp, dataset)
  
  if (format == "column")
    write_h5df_column(x, 1, h5_fp, dataset)
  
  h5close(h5_fp)
}
