# -----------------------------------------------------------------------------
# column
# -----------------------------------------------------------------------------

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
  createGroup(h5_fp, dataset)
  h5attr(h5_fp[[dataset]], "TABLE_FORMAT") = HDFIO_FORMAT_COLUMN
  h5attr(h5_fp[[dataset]], "HDFIO_VERSION") = HDFIO_VERSION
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
      h5attr(h5_fp[[glue(dataset, varname)]], "CLASS") = H5_STORAGE_STR
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
      
      h5attr(h5_fp[[glue(dataset, varname)]], "CLASS") = H5_STORAGE_LGL
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
      h5attr(h5_fp[[glue(dataset, varname)]], "CLASS") = H5_STORAGE_FAC
    }
    else if (inherits(col, "POSIXct"))
    {
      dtype = h5types$H5T_NATIVE_DOUBLE
      types[j] = H5_STORAGE_DATE
      
      dims = H5S$new(dims=length(col), maxdims=Inf)
      h5_fp[[dataset]]$create_dataset(name=varname, dtype=dtype, space=dims, gzip_level=compression)
      
      h5attr(h5_fp[[glue(dataset, varname)]], "CLASS") = H5_STORAGE_DATE
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
      else if (is.factor(col)) {
        col.fac = factor(col)
      }
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



# -----------------------------------------------------------------------------
# compound
# -----------------------------------------------------------------------------

format_df <- function(dataframe) {
  if(!is.data.frame(dataframe)) {
    stop("Object provided is not a dataframe")
  }
  if (length(class(dataframe)) > 1) {
    dataframe <- as.data.frame(dataframe)
  }
  for (j in 1:ncol(dataframe)) {
    if(class(dataframe[,j]) == "factor") {
      dataframe[,j] <- as.character(dataframe[,j])  
    }
  }
  dataframe <- dataframe 
}



comp_struc <- function(dataframe) {
  max_string <- NA
  classes <- unlist(lapply(dataframe, class))
  x <-  vector("list", length(classes))
  for (j in 1:ncol(dataframe)) {
    if(class(dataframe[,j]) =="character") {
      strings_vec_max <- get_max_str_len(dataframe[,j])
      max_string <- strings_vec_max
      x[[j]] <- H5T_STRING$new(size = as.numeric(max_string)) 
    }
    else if(class(dataframe[,j]) == "integer") {
      x[[j]] <-  h5types$H5T_NATIVE_INT  
    }
    else if (class(dataframe[,j]) == "logical") {
      x[[j]] <- H5T_LOGICAL$new() 
    }
    else {
      x[[j]] <- h5types$H5T_NATIVE_DOUBLE
    }
  }
  return(x)
}


write_h5df_compound_init = function(x, h5_fp, dataset, strlens=NULL, compression) {
  createGroup(h5_fp, dataset)
  h5attr(h5_fp[[dataset]], "TABLE_FORMAT") = HDFIO_FORMAT_COMPOUND
  h5attr(h5_fp[[dataset]], "HDFIO_VERSION") = HDFIO_VERSION
  h5attr(h5_fp[[dataset]], "VARNAMES") = names(x)

  return(NULL)
}


write_h5df_compound = function(x, start_ind, h5_fp, dataset,types) {
  df <- format_df(x)
  if (start_ind == 1){ 
    comp <- comp_struc(df)
    comp2 <- vector("list", 1L)
    comp2 <- H5T_COMPOUND$new(names(df), dtypes=comp)
    
    h5_fp[[dataset]]$create_dataset(name="data", robj = df, dtype=comp2,space=H5S$new(dims = nrow(df), maxdims = Inf))
  }
  else
  {
    indices <- start_ind:(start_ind + NROW(df) - 1)
    h5_fp[[glue(dataset, "data")]][indices] <- df
  }
}


# -----------------------------------------------------------------------------
# interface
# -----------------------------------------------------------------------------

#' write_h5df
#' 
#' @details 
#' The input dataset should be dataframe-like (one of \code{data.frame},
#' \code{data.table}, or \code{tbl_df}), or coercible to a \code{data.frame} via
#' \code{as.data.frame()}.
#' 
#' @param x
#' Input dataset.
#' @param file
#' Output file.
#' @param dataset
#' Name of the data within the HDF5 file. If none is supplied, then this will be
#' inferred from the input file name.
#' @param format
#' One of \code{column} or \code{compound}.
#' @param compression
#' HDF5 compression level. An integer between 0 (least) to 9 (most).
#' 
#' @return
#' Invisibly returns \code{TRUE}.
#' 
#' @export
write_h5df = function(x, file, dataset=NULL, format="column", compression=4)
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
  format = match.arg(tolower(format), c("column", "compound")) 
  if (data.table::is.data.table(x))
    data.table::setDF(x)
  else if (inherits(x, "tbl_df"))
    class(x) = "data.frame"
  else if (!is.data.frame(x))
    x = as.data.frame(x)
  check_df_cols(x)
  
  
  h5_fp = h5file(file, mode="a")
  h5_check_dataset(h5_fp, dataset)
  
  if (format == "column")
  {
    writer_init = write_h5df_column_init
    writer = write_h5df_column
  } 
  else if (format == "compound")
  {
    writer_init = write_h5df_compound_init
    writer = write_h5df_compound
  }
  
  types = writer_init(x, h5_fp, dataset, strlens=NULL, compression=compression)
  writer(x, 1, h5_fp, dataset, types)
  
  h5close(h5_fp)
  invisible(TRUE)
}
