# Apache Beam


> [Documentation](https://beam.apache.org/)

**Apache Beam** is an open source, unified model for defining both **batch** and **streaming** data-parallel processing pipelines.
Using the open source Beam SDKs, you build a program
defining the pipeline, that then is executed by one of Beam’s supported distributed processing backends.

**Pipeline:** is a Directed Acyclic Graph (DAG) of data transformations applied to one or more collections of data. It might include multiple input sources and output sinks and its operations (*PTransforms*) can both read and output *PCollections*.  


**PCollection:** is an immutable collection of values. It can contain a bounded or unbounded number of elements. Beam transforms use *PCollection* objects as *inputs* and *outputs*.  

+ Multiple pipelines cannot share one.  
+ Can be **bounded** (batch) or **unbounded** (streaming) in size.
+ Elements in a PCollection may be of any type, but all must be of the same type.
+ Is *immutable*. A transform might process each
element in it and generate a new PCollection, but it does modify the original one.
+ Do not support random access to elements.

**PTransform:** is an operation in a pipeline. Developers provide processing logic as a function object applied to each element of one or more input PCollection(s). Depending on the runner, multiple workers across a cluster may execute the code in parallel and generate the output elements ultimately added to the final PCollection produced by the transform.

+ To invoke a *transform*, you must apply it
to the input *PCollection*.
+ Can be nested to form composite transforms.
+ Can be chained to be applied sequentially to an input *PCollection*.
+ A transform does not alter the input collection.

--- 
**Window** subdivides a PCollection according to the timestamps of its individual elements, which is especially useful for unbounded PCollections, as it allows operating on sub-groups of elements.