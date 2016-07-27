---
layout: release
title:  "The Dev Blog"
date:   2016-07-27 13:34:00
abstract: " Today, we are starting the dev blog.  We intent to write about what we are doing and the
process that we are doing it. Without further ado, we will give a brief explanation on the project
development roadmap. What we have done so far, what we are doing and what we need to do in the
future. "
categories: [blog, news]
---

Wight now we have a partially functional framework with working examples and initial native
benchmarking tools. Faster implements a subset of the ideal functional operators for the different
dataset types available. Most of them have very low latency:

![Overall Low Latency](/faster/img/functionLatencyNP.png)

Faster is able to create datasets from disk and from memory, apply several
functional programming operators in parallel distributed across multiple machines, collect the
result or distributed data, and write on Disk.
In the examples directory in de source we have several toy examples and also some algorithms. One of
the alforithms is a Pagerank implementation that rendered up to 30x speedup over Spark.

![30x times faster than Spark](/faster/img/spark_vs_faster.png)

With our performance results we have presented an [paper] at the 16th
IEEE/ACM International Symposium on Cluster, Cloud and Grid Computing (CCGrid2016).

__Faster: a low overhead framework for massive data analysis__<br>
_Matheus C. Santos, Wagner Meira Jr., Dorgival Guedes and Virgı́lio F. Almeida_<br>
CCGrid2016

[paper]: https://ccgrid2016.uniandes.edu.co/program.html
