// Copyright (C) 2010 Oliver Schulz <oliver.schulz@tu-dortmund.de>

// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.


package daqcore.prot.rootsys

import java.util.UUID

import daqcore.util._


class ProductSerializer[A <: Product : ClassManifest]() extends ContentSerializer[A] {
  import ProductSerializer._

  val mfInfo = ManifestInfo(mf)
  val ctor = cl.getConstructors().last

  val fields: Seq[FieldIO[_]] = {
    val ctorMFs = ctor.getGenericParameterTypes map mfInfo.manifestOf toList
    val fieldMFs = cl.getDeclaredFields map {_.getGenericType} map mfInfo.manifestOf take ctorMFs.size toList
    val fieldNames = cl.getDeclaredFields map {_.getName} take ctorMFs.size toList
    
    if (fieldMFs != ctorMFs) throw new IllegalArgumentException("ProductSerializer: Can't support type " + mf + ", constructor argument and field types don't match.")
    
    for {(name, mf) <- fieldNames zip ctorMFs} yield FieldIO(name, ContentSerializer.forType(mf))
  }
  
  val flatFields: Seq[FieldIO[_]] = fields flatMap { f => f.io match {
    case ps: ProductSerializer[_] => for (g <- ps.flatFields) yield {
      g.copy(f.name + "." + g.name, g.io)
    }
    case _ => Seq(f)
  } }

  def write(out: BasicOutput, x: A) = {
    for { (f, v) <- fields.iterator zip x.productIterator }
      f.io.writeObj(out, v)
  }

  def read(in: BasicInput): A = {
    val args = fields map { _.io.readObj(in) }
    ctor.newInstance(args: _*).asInstanceOf[A]
  }
  
  override def toString() = "ProductSerializer(" + mf + "(" + fields.mkString(", ") + "))"
}


object ProductSerializer {
  def forType[A <: Product : ClassManifest] = new ProductSerializer[A]

  case class FieldIO[T](val name: String, val io: ContentSerializer[T]) {
    protected def getTypeName(mf: ClassManifest[_]): String = {
      def unsupported = new IllegalArgumentException("FieldIO does not support type " + mf)
      mf.erasure match {
        case java.lang.Boolean.TYPE => "bool"
        case java.lang.Byte.TYPE => "int8"
        case java.lang.Short.TYPE => "int16"
        case java.lang.Integer.TYPE => "int32"
        case java.lang.Long.TYPE => "int64"
        case java.lang.Float.TYPE => "float"
        case java.lang.Double.TYPE => "double"
        case cl => {
          if (cl.isArray) getTypeName(ClassOps.manifestFromClass(cl.getComponentType))
          else if (classOf[String] == cl) "string"
          else if (classOf[UUID] == cl) "uuid"
          else if (classOf[Seq[_]].isAssignableFrom(cl) && cl.isAssignableFrom(classOf[ArrayVec[_]]))
            "vector<" + getTypeName(mf.typeArguments.head.asInstanceOf[ClassManifest[_]]) + ">"
          else if (classOf[Product].isAssignableFrom(cl)) cl.shortName
          else throw unsupported
        }
      }
    }

    val typeName = getTypeName(io.mf)

    override def toString() = name + ": " + io.mf 
  }
}
