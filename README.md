# NY Taxi Rides
---

This Spark project analyzes [*taxi rides within New York*](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

The data set used is a sample from January, 2016 data. Where each row, referred to as `trip`, contains time and location for the trip's start and end.

We try to find out how many trips and return trips we can match. A `trip B` is considered as a return trip for `trip a` if:
Time between Taxi1's dropoff time and Taxi2's pickup time is not more than 8h
1. The time between `A`'s dropoff time and `B`'s pickup time is not more than 8 hours.
2. `B`'s pickup location and `A`'s dropoff location are not further than `r` meters.
3. `B`'s dropoff location and `A`'s pickup location are not further than `r` meters.