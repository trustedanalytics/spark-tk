SaveLoad
========

This is an approach sparktk uses to save and load entities.  Consider models from Spark.  They each have save/load
capability built-in.  For a given path, they save to two subfolders, data/ and metadata/.  Sparktk uses this as is,
no need to interrupt what is working.  Sparktk just add another subfolder, tk/, wherein it stores extra metadata it
finds necessary or useful to hold on to.  For example, it is useful to store all the parameters a model was trained
with, not just those that make up the final model.


Steps to make an entity able to be saved and loaded generically with Sparktk:


1. Identify the class of the entity.  This is usually a case class.

2. Identify or create a companion object for #1, or some other object which provides static functionality for
   loading and construction of the entity.  This object **must extend TkSaveableObject and implement a load method**.
   (It may also override the formatId, though this is rare).
   
3. Add a save method to #1, ``def save(sc: SparkContext, path: String): Unit``

4. Add an entry to the ``loaders`` in Loaders.scala which indicates the load method #2

5. Create a python proxy object for the class #1.  The python module path must mimic the scala package path.
   For example:
   
     Scala:  ``package org.trustedanalytics.sparktk.models.clustering.kmeans  case class KMeansModel``
     
     Python: ``sparktk.models.clustering.kmeans  class KMeansModel``
     
   The python proxy object must implement a static load method.
   
       @staticmethod
       def load(tc, scala_instance):
           return KMeansModel(tc, scala_instance)
           
           
TkSaveLoad and FormatId, FormatVersion
--------------------------------------

(SparkTk's SaveLoad uses `FormatId` and `FormatVersion`, pretty much like how Spark uses "className" and "version")

When SparkTk tries to load an arbitrary path, it looks for the tk/ folder and first loads its content.  The tk/
contains JSON which is essentially a tuple of (FormatId, FormatVersion, data)

*    *FormatId: String* - the identifier of the object this data represents.  It is almost always the name of the case
class's companion object, which is what does the loading

*    *FormatVersion: Int* - defaults to 1.  Allows the serialization format to mature while maintaining backwards comp.  This
is not automatic.  It only provides the coder the ability to branch on different versions in the load code.

*    *data: JValue* -  SparkTk, like Spark, uses json4s, which is a fairly straightforward library for working with JSON.
The data stored data is expected to be a JSON AST of this sort.


`SaveLoad` is the generic implementation surrounding this tuple and `TkSaveLoad` is a simpler wrapper which indicates the "tk/" path.

The load and save methods defined for the entity should use `TkSaveLoad` for special metadata, and then the regular
Spark load/save methods as needed.

Getting back to the load story, sparktk first load the tk/ and grabs the formatId.  It uses that to lookup a loader,
found in `Loaders.scala`.  It calls the loader (described in #2 above) and passes the SparkContext, the path,
the FormatVersion, and the TkMetadata it just loaded.

When implementing the save method, be sure to do a TkSaveLoad.saveTk to establish metadata for the sparktk loader.
Even if there is no extra metadata to save, there must still be a tk/ folder created to store the formatId.  Otherwise
sparktk won't know how to load it.

**Finally, look at KMeansModel as an example**
