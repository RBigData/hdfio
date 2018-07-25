library(hdf5r)

f_path = system.file("exampledata/pytables_fixed.h5", package="hdfio")
g_path = system.file("exampledata/pytables_table.h5", package="hdfio")

f = h5file(f_path)
g = h5file(g_path)

test = hdfio::h5_detect_format(f, "mydata")
stopifnot(identical(test, "pytables_fixed"))
test = hdfio::h5_detect_format(g, "mydata")
stopifnot(identical(test, "pytables_table"))


truth = c(5L, 4L)
test = hdfio::h5_dim(f, "mydata")
stopifnot(identical(test, truth))
test = hdfio::h5_dim(g, "mydata")
stopifnot(identical(test, truth))


truth = c("a", "b", "x", "y")
test = hdfio::h5_colnames(f, "mydata")
stopifnot(identical(test, truth))
test = hdfio::h5_colnames(g, "mydata")
stopifnot(identical(test, truth))

h5close(f)
h5close(g)
