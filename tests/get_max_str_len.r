set.seed(1234)
maxlen = 5L

fun = hdfio::get_max_str_len

lens = sample(1:maxlen, size=20, replace=T)
words = sapply(lens, function(i) paste0(sample(letters, size=i, replace=TRUE), collapse=""))

stopifnot(identical(fun(words), maxlen))

f = factor(sample(words, size=1000, replace=TRUE))
stopifnot(identical(fun(f), maxlen))



### omp test - disable for cran
# lens = sample(1:maxlen, size=5000, replace=T)
# words = sapply(lens, function(i) paste0(sample(letters, size=i, replace=TRUE), collapse=""))
# system.time(fun(words))[3]