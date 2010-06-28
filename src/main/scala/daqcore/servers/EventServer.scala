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


package daqcore.servers

import scala.actors._, scala.actors.Actor._

import daqcore.actors._
import daqcore.util._
import daqcore.profiles._


trait EventServer extends Server with EventSource {
  @volatile protected var handlers = Set.empty[EventHandler]
  
  protected class ReplyingEventHandler[T](f: PartialFunction[Any, T], replyTo: ReplyTarget) extends EventHandler {
    def handle = { case msg if f.isDefinedAt(msg) => replyTo ! f(msg); false }
  }
  
  protected def nHandlers: Int = handlers.size
  protected def hasHandlers: Boolean = ! handlers.isEmpty
  
  protected def doEmit(event: Any): Unit = {
      trace("Emitting event %s".format(loggable(event)))
      for { handler <- handlers } {
        try if (handler.handle isDefinedAt event) {
          val again = handler.handle(event)
          if (!again) doRemoveHandler(handler)
        }
        catch { case _ => doRemoveHandler(handler) }
      }
  }
  
  protected def doAddHandler(handler: EventHandler): Unit = {
    trace("Adding %s as as handler %s".format(handler, nHandlers+1))
    handlers = handlers + handler
  }
  
  protected def doRemoveHandler(handler: EventHandler): Unit = {
    trace("Removing handler %s".format(handler))
    handlers = handlers - handler
  }
  
  override protected def init() = {
    super.init()
    trapExit = true
  }

  protected def serve = {
    case EventSource.Emit(event) =>
      doEmit(event)
    case EventSource.AddHandler(handler) =>
      doAddHandler(handler)
    case EventSource.GetEvent(f) =>
      doAddHandler(new ReplyingEventHandler(f, replyTarget))
    case EventSource.RemoveHandler(handler) =>
      doRemoveHandler(handler)
  }
}