package org.trustedanalytics.sparktk.models

// from https://gist.github.com/ConnorDoyle/7002426

import org.apache.spark.SparkContext

import scala.reflect.runtime.universe._

//object ModelSaveLoad {
//
//  def load[T <: Model](sc: SparkContext, path: String): T = {
//    val factory = new CaseClassFactory[T]
//    factory.buildWith()
//  }
//}

//def load(sc: SparkContext, path: String): M

class CaseClassFactory[T: TypeTag] {

  val classLoaderMirror = runtimeMirror(getClass.getClassLoader)

  val tpe = typeOf[T]
  val classSymbol = tpe.typeSymbol.asClass

  if (!(tpe <:< typeOf[Product] && classSymbol.isCaseClass))
    throw new IllegalArgumentException(
      "CaseClassFactory only applies to case classes!"
    )

  val classMirror = classLoaderMirror reflectClass classSymbol

  val constructorSymbol = tpe.declaration(nme.CONSTRUCTOR)

  val defaultConstructor =
    if (constructorSymbol.isMethod) constructorSymbol.asMethod
    else {
      val ctors = constructorSymbol.asTerm.alternatives
      ctors.map { _.asMethod }.find { _.isPrimaryConstructor }.get
    }

  val constructorMethod = classMirror reflectConstructor defaultConstructor

  /**
   * Attempts to create a new instance of the specified type by calling the
   * constructor method with the supplied arguments.
   *
   * @param args the arguments to supply to the constructor method
   */
  def buildWith(args: Seq[_]): T = constructorMethod(args: _*).asInstanceOf[T]

}

