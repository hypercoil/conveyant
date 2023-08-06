# ``conveyant``
An ultra-lightweight system for compositional quasi-functional programming abstractions

We use this system to separate abstract or complicated core routines from specific, concrete implementations that operate with particular dataset or input / output types. For example, instead of separately handling different imaging data types and output types (e.g., screenshots and interactive HTML) in our plotting function, the preprocessors for each data type and postprocessors for each artefact type are lifted out from a plotting function that operates directly on tensor types. Different compositional chains can then transform the general and minimal core plotting routine into a pipeline that reads a desired file type and outputs each desired artefact.

Because a compositional system has the potential to radically transform function signatures, use of transformed functions can become confusing and unsafe. To remedy this, the system includes a few simple emulators that track changes to function signatures across compositions and can also be used to enforce argument checking. For example, the ``@splice_on`` decorator can be used to combine the parameters of two composed functions, while optionally occluding parameters to the outer function that are provided as outputs from the inner function. A system for automatically constructing docstrings for compositions (by building up from a library of parameter definitions) is also under development.

This system is supercharged with the addition of different "compositors", or composition operators. The default compositor is the familiar functional composition, which substitutes outputs of the first ("inner") functional primitive as the inputs of the next ("outer") primitive in the chain. Other compositors change the interpretation of functional composition. For example, an input mapper compositor replicates the inner and outer function calls across each combination of a specified set of parameter assignments. Alternatively, an output mapper compositor evaluates the inner primitive and then copies the outer primitive for each output, potentially also mapping different parameter values to each instance of the outer primitive call. Finally, a delayed execution compositor underpins a join transformation that reduces a sequence of parameters to the outer function into a scalar argument. These compositors together can operationalise a DAG-like pipeline, and future compositors might be added that automatically wrap primitives into nodes of distributed HPC-compatible graphical workflow engines like pydra.

Containers for callables enable further features, including some that must only be used with extreme caution. For instance, delayed binding of parameters to a callable can be used to create a callable that is only partially specified, and can be completed later. This is useful for creating a callable that is parameterised by a set of parameters that are not yet known, but will be known later. This differs from partial application in that the operation that performs the binding does not need to know the parameter names. However, use of this feature is generally an anti-pattern that can make code challenging to read and understand.
