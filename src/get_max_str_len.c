#include <R.h>
#include <Rinternals.h>
#include <string.h>

#define CHARPT(x,i) ((char*)CHAR(STRING_ELT(x,i)))

#define OMP_MIN_SIZE 2500


SEXP R_get_max_str_len(SEXP x)
{
  SEXP ret;
  const int n = (int) LENGTH(x);
  int maxlen = 0;
  
#ifdef _OPENMP
  #pragma omp parallel for reduction(max:maxlen) if(n>OMP_MIN_SIZE)
#endif
  for (int i=0; i<n; i++)
  {
    int len;
    
    if (STRING_ELT(x, i) == NA_STRING)
      len = 2; // "NA"
    else
    {
      const char *const restrict s = CHARPT(x, i);
      len = strlen(s);
    }
    
    if (len > maxlen)
      maxlen = len;
  }
  
  PROTECT(ret = allocVector(INTSXP, 1));
  INTEGER(ret)[0] = maxlen;
  UNPROTECT(1);
  return ret;
}
