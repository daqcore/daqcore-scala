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


package daqcore.scpi

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

import daqcore.util.ByteCSeq
import daqcore.scpi.mnemonics._


class SCPIParserSpec extends WordSpec with MustMatchers {
  "An SCPIParser" should {
    val parser = SCPIParser()

    "parse responses correctly" in {
      val i = 42
      val x = 7.3
      val s = "foo"
      val l = List(1,2,3,4)
      val bytes = "Some block data".getBytes("ASCII").toSeq.asInstanceOf[IndexedSeq[Byte]]
      val bytes2 = (0 to 511) map {_.toByte}

      val input = Response(Result(NR1(i), BlockData(bytes), SRD(s), BlockData(bytes2), NRf(x)) + l.map{NR1(_)}).charSeq

      val response = parser.parseResponse(input)
      assert( response.charSeq === input )

      val Response(Result(NR1(iR), BlockData(bytesR), SRD(sR), BlockData(bytes2R), NRf(xR), lRR @_*)) = response
      val lR = lRR map {c => val NR1(i) = c; i}
      assert( iR === i )
      assert( xR === x )
      assert( sR === s )
      assert( bytesR  === bytes )
      assert( bytes2R === bytes2 )
      assert( lR === l )
      
      val input2 = ByteCSeq("  1,  \t 2,3 ,  #15Hello , 4 ")
      val response2 = parser.parseResponse(input2)
      println(response2)
      assert( response2.charSeq === ByteCSeq("1,2,3,#15Hello,4") )
    }
    
    "parse requests correctly" in {
      parser.parseRequest(Request(IDN!).charSeq)
      parser.parseRequest(Request(IDN?).charSeq)
      val expected = ByteCSeq("*IDN?;SET:VOLT2:DC 5,5.5,#15Hello")

      val req = Request(IDN?, SET~VOLTage(2)~DC! (5, 5.5, BlockData(ByteCSeq("Hello").contents)))
      val preq = parser.parseRequest(req.charSeq)
      assert( req.charSeq === preq.charSeq )
      assert( req.charSeq === expected )
      
      val req2 = ByteCSeq("  *IDN? \t;  SET:VOLT2:DC 5 , 5.5 , #15Hello  ")
      val preq2 = parser.parseRequest(req2)
      assert( preq2.charSeq === expected )
      assert( preq === preq2 )
    }
    
    "extract terminated messaged correctly" in {
      import daqcore.util._

      val in1 = ByteCSeq("*IDN?;SET:VOLT2:DC 5,5.5,#15Hello")
      val in2 = ByteCSeq("  1,   2,3 ,  #15Hello , 4 ")
      val input = in1 ++ ByteCSeq("\n") ++
            in2 ++ ByteCSeq("\r\n") ++ ByteCSeq("7")
      val res1 = parser.extractTermMsg(input)
      val res2 = parser.extractTermMsg(res1.next)
      val res3 = parser.extractTermMsg(res2.next)
      
      assert( res1.get == in1 )
      assert( res2.get == in2 )
      assert( res1.get.contents sharedWith input.contents )
      assert( res2.get.contents sharedWith input.contents )
      assert( res3.successful === false )
    }
  }
}
