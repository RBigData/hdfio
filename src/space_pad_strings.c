#include <R.h>
#include <Rinternals.h>
#include <string.h>

#define CHARPT(x,i) ((char*)CHAR(STRING_ELT(x,i)))


static inline void space_string(const int len, char *const restrict s)
{
  for (int i=0; i<len; i++)
    s[i] = ' ';
}

static inline int strcpy_retlen(char *const restrict dest, const char *const restrict src)
{
  int len = 0;
  
  while (src[len] != '\0')
  {
    dest[len] = src[len];
    len++;
  }
  
  return len;
}



SEXP R_space_pad_strings(SEXP x)
{
  SEXP ret;
  const int n = (int) LENGTH(x);
  int maxlen = 0;
  
  for (int i=0; i<n; i++)
  {
    const char *const restrict s = CHARPT(x, i);
    int len = strlen(s);
    
    if (len > maxlen)
      maxlen = len;
  }
  
  PROTECT(ret = allocVector(STRSXP, n));
  
  char *tmp = malloc(maxlen * sizeof(*tmp));
  if (tmp == NULL)
    error("OOM\n");
  
  for (int i=0; i<n; i++)
  {
    const char *const restrict s = CHARPT(x, i);
    int len = strcpy_retlen(tmp, s);
    space_string(maxlen-len, tmp+len);
    SET_STRING_ELT(ret, i, mkCharLen(tmp, maxlen));
  }
  
  free(tmp);
  
  UNPROTECT(1);
  return ret;
}
