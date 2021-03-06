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


package daqcore

import scala.swing._
import javax.swing.{SwingUtilities, UIManager}

package object gui {
  def setLook(): Unit = {
    Swing.onEDT {
      UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName())
    }
  }
  
  def asyncOnEDT(body: => Unit): Unit = Swing.onEDT { body }

  def synchedOnEDT[A](body: => A): A = {
    if (SwingUtilities.isEventDispatchThread()) body
    else {
      var result: Option[A] = None
      var exception: Throwable = null
      
      val lock = new {}
      lock.synchronized {
        Swing.onEDT {
          lock.synchronized {
            try { result = Some(body) }
            catch { case e: Throwable => exception = e }
            finally { lock.notify() }
          }
        }
        lock.wait()
        result getOrElse {throw exception}
      }
    }
  }
}
