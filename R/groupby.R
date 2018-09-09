groupby = function(x, y, by) {
  stopifnot(is.data.table(x),
            is.character(y), length(y)==1L, y%in%names(x),
            is.character(by), by%in%names(x))
  o = forderv(x, by=by, retGrp=TRUE)
  grps = attr(o, "starts", TRUE)
  xo = x[o, y, with=FALSE]
  grps_end = c((grps-1L)[-1L], nrow(x))
  ans = rep(0, length(grps))
  # parallel pragma here
  for (i in 1:length(grps)) {
    s = 0
    for (j in grps[i]:grps_end[i]) s = s + xo[j][[y]]
    ans[i] = s
  }
  ans
  #.Call("groupby", x, by)
}
if (dev<-FALSE) {
  cc(F)
  set.seed(108)
  x = data.table(V1=rnorm(10), V2=sample(6, 10, replace = TRUE))
  y = "V1"
  by = "V2"
  all.equal(groupby(x, y, by), x[, sum(V1), keyby=V2][["V1"]])
}
