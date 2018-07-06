check_df_cols = function(df)
{
  for (j in 1:NCOL(df))
  {
    type = class(df[, j])[1]
    if (type != "numeric" && type != "integer" && type != "logical" && type != "character" && type != "factor" && type != "POSIXct" && type != "POSIXt")
      stop("dataframe includes non-atomic/non-factor/non-date type")
  }
}




write_h5df_column_init = function(x, h5_fp, dataset, strlens=NULL, compression)
{
  h5attr(h5_fp, "TABLE_FORMAT") = "hdfio_column"
  h5attr(h5_fp, "HDFIO_VERSION") = HDFIO_VERSION
  
  createGroup(h5_fp, dataset)
  h5attr(h5_fp[[dataset]], "VARNAMES") = names(x)
  
  types = integer(ncol(x))
  
  
  for (j in 1:ncol(x))
  {
    varname = paste0("x", j)
    col = x[, j]
    class = class(col)[1]
    
    if (class == "character" || (!is.null(strlens[j]) && strlens[j] > 0))
    {
      if (is.null(strlens))
        len = get_max_str_len(col)
      else
        len = strlens[j]
      
      str_fixed_len = H5T_STRING$new(size = len)
      
      dims = H5S$new(dims=length(col), maxdims=Inf)
      h5_fp[[dataset]]$create_dataset(name=varname, dtype=str_fixed_len, space=dims, gzip_level=compression)
      
      types[j] = H5_STORAGE_STR
    }
    else if (class == "numeric" || class == "integer")
    {
      if (typeof(col) == "double")
      {
        dtype = h5types$H5T_NATIVE_DOUBLE
        types[j] = H5_STORAGE_DBL
      }
      else
      {
        dtype = h5types$H5T_NATIVE_INT
        types[j] = H5_STORAGE_INT
      }
      
      dims = H5S$new(dims=length(col), maxdims=Inf)
      h5_fp[[dataset]]$create_dataset(name=varname, dtype=dtype, space=dims, gzip_level=compression)
    }
    else if (class == "logical")
    {
      dtype = H5T_LOGICAL$new()
      types[j] = H5_STORAGE_LGL
      
      dims = H5S$new(dims=length(col), maxdims=Inf)
      h5_fp[[dataset]]$create_dataset(name=varname, dtype=dtype, space=dims, gzip_level=compression)
      
      h5attr(h5_fp[[glue(dataset, varname)]], "CLASS") = "logical"
    }
    else if (class == "factor")
    {
      dtype = h5types$H5T_NATIVE_INT
      types[j] = H5_STORAGE_FAC
      
      dims = H5S$new(dims=length(col), maxdims=Inf)
      h5_fp[[dataset]]$create_dataset(name=varname, dtype=dtype, space=dims, gzip_level=compression)
      
      # TODO merge factor levels on successive writes?
      levels = levels(col)
      h5attr(h5_fp[[glue(dataset, varname)]], "LEVELS") = levels
      h5attr(h5_fp[[glue(dataset, varname)]], "CLASS") = "factor"
    }
    else if (inherits(col, "POSIXct"))
    {
      dtype = h5types$H5T_NATIVE_DOUBLE
      types[j] = H5_STORAGE_DATE
      
      dims = H5S$new(dims=length(col), maxdims=Inf)
      h5_fp[[dataset]]$create_dataset(name=varname, dtype=dtype, space=dims, gzip_level=compression)
      
      h5attr(h5_fp[[glue(dataset, varname)]], "CLASS") = "date"
    }
    else
      close_and_stop(h5_fp, INTERNAL_ERROR)
  }
  
  types
}



write_h5df_column = function(x, start_ind, h5_fp, dataset, types)
{
  for (j in 1:ncol(x))
  {
    varname = paste0("x", j)
    col = x[, j]
    
    if (types[j] == H5_STORAGE_FAC)
    {
      if (is.character(col))
        col.fac = factor(col) # TODO grab levels
      
      col = as.integer(col.fac)
    }
    else if (types[j] == H5_STORAGE_STR)
    {
      if (!is.character(col))
        col = as.character(col)
    }
    else if (types[j] == H5_STORAGE_DATE)
      col = as.double(col)
    
    h5_fp[[glue(dataset, varname)]][start_ind : (start_ind+length(col)-1)] = col
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
write_h5df = function(x, file, dataset=NULL, format="column", compression=0)
{
  check.is.string(file)
  if (!is.null(dataset))
    check.is.string(dataset)
  else
    dataset = deparse(substitute(x))
  check.is.string(format)
  check.is.natnum(compression)
  if (compression > 9)
    stop("argument 'compression' must be an integer from 0 to 9 (inclusive)")
  format = match.arg(tolower(format), c("column")) # TODO compound
  if (data.table::is.data.table(x))
    data.table::setDF(x)
  else if (inherits(x, "tbl_df"))
    class(x) = "data.frame"
  else if (!is.data.frame(x))
    stop("argument 'x' must be a data.frame or data.table object")
  check_df_cols(x)
  
  h5_fp = h5file(file, mode="a")
  h5_check_dataset(h5_fp, dataset)
  
  if (format == "column")
  {
    types = write_h5df_column_init(x, h5_fp, dataset, strlens=NULL, compression=compression)
    write_h5df_column(x, 1, h5_fp, dataset, types)
  }
  
  h5close(h5_fp)
}
