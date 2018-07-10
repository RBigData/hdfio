# hdfio

* **Version:** 0.1-0
* **Status:** [![Build Status](https://travis-ci.org/RBigData/hdfio.png)](https://travis-ci.org/RBigData/hdfio)
* **License:** [BSD 2-Clause](http://opensource.org/licenses/BSD-2-Clause)
* **Author:** Drew Schmidt and Amil Williamson


A set of high-level utilities for working with [HDF5](https://www.hdfgroup.org/).

This package is not meant to expose anywhere near the full capabilities of HDF5. For that, see the [rhdf5](https://www.bioconductor.org/packages/release/bioc/html/rhdf5.html) and [hdf5r](https://cran.r-project.org/web/packages/hdf5r/index.html) packages (we actually use hdf5r internally). The goal of this package is to try to make HDF5 as simple to use as `read.csv()` and `write.csv()`, but with the added benefits of using binary file formats.

Our main focus is on storing and reading dataframes. We call these "h5df" as in "h5 dataframe". I understand that this is annoying and difficult to convince your fingers to type if you are familiar with HDF5, but it's too good of a name to pass up. Right now we support reading from two kinds of formats written by python's pandas (really pytables), with some restrictions (no strings when `format=fixed`). We have full support for a format that is good for working with R, and should soon have a format that is useful if the goal is to regularly share data between python and R.

The current documentation is a train wreck, but we're working on it.



## Installation

<!-- To install the R package, run:

```r
install.package("hdfio")
``` -->

The development version is maintained on GitHub, and can easily be installed by any of the packages that offer installations from GitHub:

```r
remotes::install_github("wrathematics/lineSampler")
remotes::install_github("RBigData/hdfio")
```



## Examples

We'll take a look at some examples using the famous [airlines dataset](http://stat-computing.org/dataexpo/2009/), and we'll be working from a directory that has all of the uncompressed csv's:

```r
dir(pattern="*.csv")
##  [1] "1987.csv" "1988.csv" "1989.csv" "1990.csv" "1991.csv" "1992.csv"
##  [7] "1993.csv" "1994.csv" "1995.csv" "1996.csv" "1997.csv" "1998.csv"
## [13] "1999.csv" "2000.csv" "2001.csv" "2002.csv" "2003.csv" "2004.csv"
## [19] "2005.csv" "2006.csv" "2007.csv" "2008.csv"
```

The hdfio package has several ways of taking a csv and turning it into an HDF5 file. We'll start with the basic `write_hdf5()` interface, which takes a dataframe that has already been read into memory and writes it out to HDF5:

```r
library(hdfio)

airlines1987 = data.table::fread("1987.csv")
write_h5df(airlines1987, "/tmp/airlines.h5")
```

The default format supported by the package allows us to read subsets of rows and columns back in:

```r
read_h5df("/tmp/airlines.h5", rows=1:5, cols=1:8)
##   Year Month DayofMonth DayOfWeek DepTime CRSDepTime ArrTime CRSArrTime
## 1 1987    10         14         3     741        730     912        849
## 2 1987    10         15         4     729        730     903        849
## 3 1987    10         17         6     741        730     918        849
## 4 1987    10         18         7     729        730     847        849
## 5 1987    10         19         1     749        730     922        849
```

Note that the `read_h5df()` function supports only a few formats (including several employed by python's pandas). It does not handle an arbitrary HDF5 file.

We can also get a quick summary of the dataset:

```r
summarize_h5df("/tmp/airlines.h5")
## Filename:    /tmp/airlines.h5 
## File size:   10.710 MiB 
## Datasets:
##     airlines1987 
##         Format:     hdfio_column 
##         Dimensions: 1311826 x 29 
```

Each HDF5 file can support multiple datasets:

```r
airlines1988 = data.table::fread("1988.csv")
write_h5df(airlines1988, "/tmp/airlines.h5")
```

If there are multiple datasets, you have to specify which one you want with the reader (otherwise it will just automatically pick the one available):

```r
read_h5df("/tmp/airlines.h5", rows=1:5, cols=1:8)
## Error in h5_get_dataset(h5_fp, dataset) : multiple datasets available

read_h5df("/tmp/airlines.h5", dataset="airlines1988", rows=1:5, cols=1:8)
##   Year Month DayofMonth DayOfWeek DepTime CRSDepTime ArrTime CRSArrTime
## 1 1988     1          9         6    1348       1331    1458       1435
## 2 1988     1         10         7    1334       1331    1443       1435
## 3 1988     1         11         1    1446       1331    1553       1435
## 4 1988     1         12         2    1334       1331    1438       1435
## 5 1988     1         13         3    1341       1331    1503       1435
```

Although `summarize_h5df()` will show all datasets by default, but we can specify a single dataset, and also optionally show all the column names:

```r
summarize_h5df("/tmp/airlines.h5")
## Filename:    /tmp/airlines.h5 
## File size:   52.090 MiB 
## Datasets:
##     airlines1987 
##         Format:     hdfio_column 
##         Dimensions: 1311826 x 29 
##     airlines1988 
##         Format:     hdfio_column 
##         Dimensions: 5202096 x 29 

summarize_h5df("/tmp/airlines.h5", "airlines1988", colnames=TRUE)
## Filename:    /tmp/airlines.h5 
## File size:   52.090 MiB 
## Datasets:
##     airlines1988 
##         Format:     hdfio_column 
##         Dimensions: 5202096 x 29 
##         Columns:     
##             1. Year
##             2. Month
##             3. DayofMonth
##             4. DayOfWeek
##             5. DepTime
##             6. CRSDepTime
##             7. ArrTime
##             8. CRSArrTime
##             9. UniqueCarrier
##             10. FlightNum
##             11. TailNum
##             12. ActualElapsedTime
##             13. CRSElapsedTime
##             14. AirTime
##             15. ArrDelay
##             16. DepDelay
##             17. Origin
##             18. Dest
##             19. Distance
##             20. TaxiIn
##             21. TaxiOut
##             22. Cancelled
##             23. CancellationCode
##             24. Diverted
##             25. CarrierDelay
##             26. WeatherDelay
##             27. NASDelay
##             28. SecurityDelay
##             29. LateAircraftDelay
```

The other way to transform a csv file into an HDF5 file is the `csv2h5()` function:

```r
csv2h5("1995.csv", h5out="/tmp/airlines1995.h5", verbose=TRUE)
## Processing batch 1/1
```

As the output implies, the function will process the csv file in batches if the entire dataset can't fit into RAM. This dataset is small enough that it easily fits into RAM in one batch on this computer, so it processes the entire file in 1 batch.  If a dataset name is not specified, one will be inferred from the name of the input file. This dataset is named `1995` since the input file was `1995.csv` and a dataset name (for the HDF5 file) wasn't specificed:

```r
summarize_h5df("/tmp/airlines1995.h5")
## Filename:    /tmp/airlines1995.h5 
## File size:   66.188 MiB 
## Datasets:
##     1995 
##         Format:     hdfio_column 
##         Dimensions: 5327435 x 29 
```

Finally, we can convert a directory of csv files into a single HDF5 file using `dir2h5()`. We can do this in two ways. One will combine the files into a single dataset, conceptually equivalent to reading all of the datasets and `rbind`-ing them all together. This is done one file at a time to accommodate large directories of files, and we assume that the csv files are "easily" stackable (no dropping/swapping columns across files). Doing this, we can create a single HDF5 file for the airlines dataset:

```r
file.remove("/tmp/airlines.h5")
dir2h5(".", h5out="/tmp/airlines.h5", dataset="airlines", verbose=TRUE)
## Checking input files for common header lines...ok!
## Scanning all input files for storage info...ok!
## Processing 22 files:
##     ./1987.csv: reading...ok! writing...ok!
##     ./1988.csv: reading...ok! writing...ok!
##     ./1989.csv: reading...ok! writing...ok!
##     ./1990.csv: reading...ok! writing...ok!
##     ./1991.csv: reading...ok! writing...ok!
##     ./1992.csv: reading...ok! writing...ok!
##     ./1993.csv: reading...ok! writing...ok!
##     ./1994.csv: reading...ok! writing...ok!
##     ./1995.csv: reading...ok! writing...ok!
##     ./1996.csv: reading...ok! writing...ok!
##     ./1997.csv: reading...ok! writing...ok!
##     ./1998.csv: reading...ok! writing...ok!
##     ./1999.csv: reading...ok! writing...ok!
##     ./2000.csv: reading...ok! writing...ok!
##     ./2001.csv: reading...ok! writing...ok!
##     ./2002.csv: reading...ok! writing...ok!
##     ./2003.csv: reading...ok! writing...ok!
##     ./2004.csv: reading...ok! writing...ok!
##     ./2005.csv: reading...ok! writing...ok!
##     ./2006.csv: reading...ok! writing...ok!
##     ./2007.csv: reading...ok! writing...ok!
##     ./2008.csv: reading...ok! writing...ok!
## done!

summarize_h5df("/tmp/airlines.h5")
## Filename:    /tmp/airlines.h5 
## File size:   1.345 GiB 
## Datasets:
##     airlines 
##         Format:     hdfio_column 
##         Dimensions: 123534969 x 29 
```

The process can take a lot of time, as every single file has to be scanned first to ensure type safety for the writer. This can be disabled by passing `yolo=TRUE`, but the process may fail in that case (it does with this example). 

If you just want to drop a bunch of different (possibly unrelated) csv files into a single HDF5 file, you can use `dir2h5()` with `combined=FALSE`. In this case:

```r
dir2h5(".", h5out="/tmp/airlines_split.h5", combined=FALSE)

summarize_h5df("/tmp/airlines_split.h5")
## Filename:    /tmp/airlines_split.h5 
## File size:   1.611 GiB 
## Datasets:
##     1987 
##         Format:     hdfio_column 
##         Dimensions: 1311826 x 29 
##     1988 
##         Format:     hdfio_column 
##         Dimensions: 5202096 x 29 
##     1989 
##         Format:     hdfio_column 
##         Dimensions: 5041200 x 29 
##     1990 
##         Format:     hdfio_column 
##         Dimensions: 5270893 x 29 
##     1991 
##         Format:     hdfio_column 
##         Dimensions: 5076925 x 29 
##     1992 
##         Format:     hdfio_column 
##         Dimensions: 5092157 x 29 
##     1993 
##         Format:     hdfio_column 
##         Dimensions: 5070501 x 29 
##     1994 
##         Format:     hdfio_column 
##         Dimensions: 5180048 x 29 
##     1995 
##         Format:     hdfio_column 
##         Dimensions: 5327435 x 29 
##     1996 
##         Format:     hdfio_column 
##         Dimensions: 5351983 x 29 
##     1997 
##         Format:     hdfio_column 
##         Dimensions: 5411843 x 29 
##     1998 
##         Format:     hdfio_column 
##         Dimensions: 5384721 x 29 
##     1999 
##         Format:     hdfio_column 
##         Dimensions: 5527884 x 29 
##     2000 
##         Format:     hdfio_column 
##         Dimensions: 5683047 x 29 
##     2001 
##         Format:     hdfio_column 
##         Dimensions: 5967780 x 29 
##     2002 
##         Format:     hdfio_column 
##         Dimensions: 5271359 x 29 
##     2003 
##         Format:     hdfio_column 
##         Dimensions: 6488540 x 29 
##     2004 
##         Format:     hdfio_column 
##         Dimensions: 7129270 x 29 
##     2005 
##         Format:     hdfio_column 
##         Dimensions: 7140596 x 29 
##     2006 
##         Format:     hdfio_column 
##         Dimensions: 7141922 x 29 
##     2007 
##         Format:     hdfio_column 
##         Dimensions: 7453215 x 29 
##     2008 
##         Format:     hdfio_column 
##         Dimensions: 7009728 x 29 
```

Notice the effectiveness of the default compression level (`compression=4`) here:

```r
library(memuse)

Sys.dirsize(".")
## 11.203 GiB

Sys.filesize("/tmp/airlines.h5")
## 1.345 GiB

Sys.filesize("/tmp/airlines_split.h5")
## 1.611 GiB
```

So the uncompressed csv files are just over 11 GiB, while the compressed HDF5 files are roughly 1.5 GiB. This compression makes creating the file and reading from it somewhat slower, but the performance/size tradeoff is actually quite good. The highest compression level is 9, which is slower to read and significantly slower to write, while only cutting off a few hundred MiB of space from the `compression=4` variant so we won't bother to show it here. We can compare the compressed version to an uncompressed version, but we have to rewrite the file:

```r
dir2h5(".", dataset="airlines", h5out="/tmp/airlines_nocompression.h5", compression=0)
Sys.filesize("/tmp/airlines_nocompression.h5")
## 10.647 GiB
```

It's worth pointing out that this machine doesn't have nearly that much RAM (so it is indeed being processed in chunks):

```r
Sys.meminfo()
## Totalram:  7.697 GiB 
## Freeram:   5.519 GiB 
```




## Supported Formats and Naming Conventions

TODO
