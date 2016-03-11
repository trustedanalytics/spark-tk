from sparktk.dtypes import dtypes

def schema_to_scala(schema, ctx):
    s = map(lambda t: [t[0], dtypes.to_string(t[1])], schema)  # convert dtypes to strings
    return ctx._jvm.org.trustedanalytics.at.serial.PythonSerialization.frameSchemaToScala(s)

def schema_from_scala(schema, ctx):
    s = ctx._jvm.org.trustedanalytics.at.serial.PythonSerialization.frameSchemaToPython(schema)
    return [(name, dtypes.get_from_string(dtype)) for name, dtype in s]

