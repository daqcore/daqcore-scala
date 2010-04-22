// Copyright (C) 2010 Oliver Schulz <oliver.schulz@tu-dortmund.de>,

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

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers


class ServerSpec extends WordSpec with MustMatchers {
  "A Server" should {
    object testenv {
      trait FooDevice extends Profile {
        import FooDevice._
        def foo(v: Int) = as[Int] {srv !? Foo(v)}
      }

      object FooDevice {
        case class Foo(v: Int)
      }


      trait BarDevice extends Profile {
        import BarDevice._
        def bar(v: Int) = as[Int] {srv !? Bar(v)}
      }

      object BarDevice {
        case class Bar(v: Int)
      }


      trait BazDevice extends Profile {
        def baz(v: Int) = as[Int] {srv !? None }
      }


      class FooBarActor extends Server with FooDevice with BarDevice {
        import FooDevice._, BarDevice._

        var i = 0
      
        override def init() = { i = 2}

        def serve = {
          case Foo(v) => reply(v * i)
          case Bar(v) => reply(v + i)
        }
      }
    }

    "behave correctly" in {
      import testenv._
      
      val a = new FooBarActor
      a.start
      
      assert( a.foo(4) === 8 )
      assert( a.bar(4) === 6 )
      
      val p = new ServerProxy(a) with FooDevice with BarDevice
      assert( p.foo(4) === 8 )
      assert( p.bar(4) === 6 )

      intercept[IllegalArgumentException] {
        new ServerProxy(a) with BazDevice
      }
    }
  }
}