# ``conveyant``
A minimal system for compositional functional programming abstractions

We use this system to separate abstract or complicated core routines from specific, concrete implementations that operate with particular dataset or input / output types. For example, instead of separately handling different imaging data types and output types (e.g., screenshots and interactive HTML) in our plotting function, the preprocessors for each data type and postprocessors for each artefact type are lifted out from a plotting function that operates directly on tensor types. Different compositional chains can then transform the general and minimal core plotting routine into a pipeline that reads a desired file type and outputs each desired artefact.

This system is supercharged with the addition of different "compositors", or composition operators. The default compositor is the familiar functional composition, which substitutes outputs of the first ("inner") functional primitive as the inputs of the next ("outer") primitive in the chain. Other compositors change the interpretation of functional composition. For example, an input mapper compositor replicates the inner and outer function calls across each combination of a specified set of parameter assignments. Apparently, an output mapper compositor evaluates the inner primitive and then copies the outer primitive for each output, potentially also mapping different parameter values to each instance of the outer primitive call. Finally, a delayed execution compositor underpins a join transformation that reduces a sequence of parameters to the outer function into a scalar argument. These compositors together can operationalise a DAG-like pipeline, and future compositors might be added that automatically wrap primitives into nodes of distributed HPC-compatible graphical workflow engines like pydra.
