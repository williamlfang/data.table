# R prototype to describe C logic, using sum
groupby = function(x, y, by) {
  stopifnot(is.data.table(x),
            is.character(y), length(y)==1L, y%in%names(x),
            is.character(by), by%in%names(x))
  o = forderv(x, by=by, retGrp=TRUE)
  xo = x[o, y, with=FALSE]
  grps = attr(o, "starts", TRUE)
  grps_end = c((grps-1L)[-1L], nrow(x))
  ans = rep(0, length(grps))
  # parallel pragma here
  for (i in 1:length(grps)) {
    s = 0
    for (j in grps[i]:grps_end[i]) s = s + xo[j][[y]]
    ans[i] = s
  }
  ans
}

# this is for testing prototype only
if (dev<-FALSE) {
  cc(F)
  set.seed(108)
  x = data.table(V1=rnorm(10), V2=sample(6, 10, replace = TRUE))
  y = "V1"
  by = "V2"
  all.equal(groupby(x, y, by), x[, sum(V1), keyby=V2][["V1"]])
}

# this proper C implementation
groupbyC = function(x, y, by, dev=FALSE) {
  stopifnot(is.data.table(x),
            is.character(y), length(y)==1L, y%in%names(x),
            is.character(by), by%in%names(x),
            is.double(x[[y]]))
  ans = .Call("CgroupbyR", x, match(y, names(x)), match(by, names(x)))
  return(if (!dev) ans[[4L]] else setattr(ans, "names", c("order","grp_start","group_end","ans"))[])
}

# testing and benchmarking C implementation
if (dev<-FALSE) {
  cc(F)
  set.seed(108)
  N = 1e9
  x = data.table(V1=rnorm(N), V2=sample(as.integer(N/2), N, replace = TRUE))
  x[1,1] # avoid overhead on first call after `cc`
  y = "V1"
  by = "V2"
  system.time(ans1<-data.table:::groupbyC(x, y, by, dev=FALSE))
  system.time(ans2<-x[, sum(V1), keyby=V2][["V1"]])
  all.equal(ans1, ans2)
  # N=1e1: 0.002 vs 0.027 (still `[` has some overhead)
  # N=1e7: 0.452 vs 0.993 (4 cores)
  # N=1e8: 4.8 vs 10.6 (4 cores)
  # N=1e8: 5.3 vs 12 (20 cores)
  # N=1e9: 56 vs 150 (20 cores)
}
