# FailoverManager

Failovermanager is responsible for sending nacked jobs to Retry Queue/DLQ.
It decides the destination based on job retrypolicy and attempts.

There is one FOM per worklet.