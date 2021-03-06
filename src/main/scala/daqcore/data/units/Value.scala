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


package daqcore.data.units


sealed abstract class Value {
  def v: Double
  def unit: Option[PhysUnit]
}


case class Unitless(v:Double) extends Value {
  def unit = None
  override def toString = v.toString
  def ~(u: PhysUnit) = WithUnit(v, u)
}


case class WithUnit(v: Double, u: PhysUnit) extends Value {
  def unit = Some(u)
  override def toString = v.toString + " " + u.symbol.name
}
