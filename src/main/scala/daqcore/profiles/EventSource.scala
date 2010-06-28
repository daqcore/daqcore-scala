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


package daqcore.profiles

import daqcore.actors._


trait EventHandler {
  def handle: PartialFunction[Any, Boolean]
}

object EventHandler {
  def apply(f: PartialFunction[Any, Boolean]): EventHandler =
    new EventHandler { def handle = f }
}


trait EventSource extends Profile {
  def addHandler(handler: EventHandler): EventHandler = {
    srv ! EventSource.AddHandler(handler)
    handler
  }

  def addHandlerFunc(f: PartialFunction[Any, Boolean]): EventHandler =
    addHandler(new EventHandler { def handle = f })

  def removeHandler(handler: EventHandler): Unit =
    srv ! EventSource.RemoveHandler(handler)
  
  def getEventF[T](f: PartialFunction[Any, T]): Ft[T] =
    srv.!!?(EventSource.GetEvent(f)).asInstanceOf[Ft[T]]
}


object EventSource {
  val Identity: PartialFunction[Any, Any] = { case a => a }

  case class AddHandler(handler: EventHandler)
  case class GetEvent[T](f: PartialFunction[Any, T])
  case class RemoveHandler(handler: EventHandler)
}


trait EventSender extends Profile {
  def emitEvent(event: Any): Unit =
    srv ! EventSender.Emit(event)
}


object EventSender {
  case class Emit(event: Any)
}
