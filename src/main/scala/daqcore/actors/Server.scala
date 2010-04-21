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


package daqcore.actors


import scala.actors._, scala.actors.Actor._

import daqcore.util._


trait ServerAccess extends Logging {
  def srv: Actor

  val profiles: Set[ProfileInfo]

  protected def supports(profile: ProfileInfo) = profiles.contains(profile)
  
  def requireProfile(p: ProfileInfo): Unit =
    if (!supports(p)) throw new IllegalArgumentException("Proxy target actor does not support profile " + p)

  def as[T](body: => Any) = (body).asInstanceOf[T]
}



trait Profile extends ServerAccess



trait Server extends ServerAccess with DaemonActor with Profile {
  import Server._

  def srv: Actor = this
  
  val profiles: Set[ProfileInfo] =
    ProfileInfo.profilesOf(this.getClass)

  private[actors] var restarted = false

  /** Servers may override this */
  protected def onStart(): Unit =
    { debug("Server %s started".format(self)) }

  /** Servers may override this */
  protected def onRestart(): Unit =
    { debug("Server %s restarted".format(self)) }

  /** Servers may override this */
  protected def init(): Unit =
    { trace("Server %s init".format(self)) }

  /** Servers must implement this */
  protected def serve: PartialFunction[Any, Unit]

  /** Servers may override this, deinit is called once for every call of init */
  protected def deinit(): Unit =
    { trace("Server %s deinit".format(self)) }

  /** Servers may override this */
  protected def onKill(reason: AnyRef): Unit =
    { debug("Server %s killed: %s".format(reason)) }

  //** Servers may override this */
  protected def onShutdown(): Unit =
    { debug("Server %s shut down".format(self)) }
  
  protected[actors] def handleGenericPre: PartialFunction[Any, Unit] = {
    case GetProfiles => reply(profiles)
  }

  protected[actors] def handleGenericPost: PartialFunction[Any, Unit] = {
    case Exit(_, 'normal) => 
    case e @ Exit(_, reason) => { exit(reason) }
    case _ => throw new RuntimeException("unknown message")
  }
  
  def act() = {
    exitMonitor.start()
    exitMonitor !? 'ready
    
    trapExit = true
    
    if (!restarted) { restarted = true; onStart() } else onRestart()
    init()
    
    eventloop (
      handleGenericPre orElse
      serve orElse
      handleGenericPost
    )
  }

  lazy val exitMonitor = new ExitMonitor
  
  protected class ExitMonitor extends DaemonActor with Logging {
    trapExit = true

    def act = {
      trace("Exit Monitor started")
      link(srv)
      ready()
    }

    protected def ready() = react {
      case 'ready => {
        trace("Received 'ready")
        reply()
        waitForExit()
      }
      case _ => throw new RuntimeException("ExitMonitor: unexpexted message")
    }
    
    protected def waitForExit() = react {
      case Exit(from, reason) if (from == srv) => {
        trace("Received Exit(srv,%s)".format(reason))
        try {
          trace("Trying deinit()")
          deinit()
        } catch { case _ => }
        reason match {
          case 'normal => try { onShutdown() } catch { case e => error(e) }
          case reason => try { onKill(reason) } catch { case e => error(e) }
        }
      }
      case _ => throw new RuntimeException("ExitMonitor: unexpexted message")
    }
  }
}


object Server {
  case object GetProfiles
}



class ServerProxy(val srv: Actor) extends ServerAccess {
  lazy val profiles = as[Set[ProfileInfo]] (srv !? Server.GetProfiles)

  requireProfile(ProfileInfo.apply[Server])

  ProfileInfo.profilesOf(this.getClass) foreach {requireProfile(_)}
}
