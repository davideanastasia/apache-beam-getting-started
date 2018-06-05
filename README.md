# Apache Beam Getting Started

This is 3-2-1-go project on how to get started with Apache Beam.

## Inverted Index

The idea behind this simple batch job is to create an inverted index: given a set of documents in text format, the job will parse and build a word -> location mapping for each of the words.
The job is an interesting toy, as it shows how:
- read data + file name (slightly different than using TextIO)
- filter out common stop words (in a very naive way, but more interesting ways can be found!)
- create a CombineFn in order to avoid streaming all the data for a single key to a single node

## References

- https://beam.apache.org/documentation/programming-guide/
- https://beam.apache.org/documentation/pipelines/test-your-pipeline/
- https://beam.apache.org/blog/2016/10/20/test-stream.html
- https://beam.apache.org/get-started/wordcount-example/
- https://dzone.com/articles/making-data-intensive-processing-efficient-and-por
- https://beam.apache.org/contribute/ptransform-style-guide/
- http://www.waitingforcode.com/apache-beam/joins-apache-beam/read

