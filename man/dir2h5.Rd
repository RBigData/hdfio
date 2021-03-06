% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dir2h5.r
\name{dir2h5}
\alias{dir2h5}
\title{dir2h5}
\usage{
dir2h5(csvdir, h5out, dataset = NULL, recursive = FALSE,
  combined = TRUE, header = "all", format = "column",
  compression = 4, yolo = FALSE, verbose = FALSE, ...)
}
\arguments{
\item{csvdir}{Valid directory containing csv files}

\item{h5out}{Output file.}

\item{dataset}{Name of the data within the HDF5 file. If none is supplied, then this will be
inferred from the input file name.}

\item{recursive}{TODO}

\item{combined}{Should the csv files will be writen as a single HDF5 dataset?}

\item{header}{TODO}

\item{format}{One of \code{column} or \code{compound}.}

\item{compression}{HDF5 compression level. An integer between 0 (least) to 9 (most).}

\item{yolo}{Do you want to skip input file checks? Faster, but dangerous.}

\item{verbose}{Option to enable}

\item{...}{Additional arguments passed to \code{fread()}. Can not include \code{file},
\code{skip}, \code{nrows}, \code{verbose}, \code{showProgress}, or
\code{data.table}.}
}
\value{
Invisibly returns \code{TRUE}.
}
\description{
Convert a directory of csv files to HDF5 datasets.
}
\details{
TODO
}
