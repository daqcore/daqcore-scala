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


package daqcore.data

import java.util.UUID

import daqcore.util._


case class FlatEvent (
  info: Event.Info,
  raw: FlatEvent.FlatRaw
) {
  def toEvent: Event = Event(
    info = info,
    raw = raw.toRaw
  )
}


object FlatEvent {
  implicit private def seqIntToArrayVecInt(seq: Seq[Int]): ArrayVecInt = seq.toArrayVec

  def apply(event: Event): FlatEvent = {
    FlatEvent(
      info = event.info,
      raw = FlatRaw(event.raw)
    )
  }


  case class FlatRaw (
    trig: ArrayVecInt,
    trans: FlatRaw.FlatTransients,
    flags: Int
  ) {
    def toRaw = Event.Raw(
      trig = trig,
      trans = trans.toTransients,
      flags = flags
    )
  }


  object FlatRaw {
    import Event.Raw._

    def apply(raw: Event.Raw): FlatRaw = FlatRaw (
      trig = raw.trig,
      trans = FlatTransients(raw.trans),
      flags = raw.flags
    )


    case class FlatTransients (
      trigPos: ArrayVecInt,
      samples_ch: ArrayVecInt,
      samples_val_n: ArrayVecInt,
      samples_val: ArrayVecShort
    ) {
      def toTransients: Map[Int, Transient] = {
        val channelIt = samples_ch.iterator
        val trigPosIt = trigPos.iterator
        val nSamplesIt = samples_val_n.iterator

        var transients = Map.empty[Int, Transient]
        var offset = 0

        while(channelIt.hasNext) {
          val channel = channelIt.next
          val trigPos = trigPosIt.next
          val nSamples = nSamplesIt.next
          val samples = (for {v <- samples_val.view.slice(offset, offset + nSamples)} yield v.toInt).toArrayVec
          transients = transients + (channel -> Transient(trigPos, samples))
          offset = offset + nSamples
        }
        
        transients
      }
    }

    object FlatTransients {
      def apply(transients: Map[Int, Transient]): FlatTransients = {
        val transCh = transients.keys.toSeq.sortWith{_ < _}

        FlatTransients(
          samples_ch = for (ch <- transCh) yield ch,
          trigPos = for (ch <- transCh) yield transients(ch).trigPos,
          samples_val_n = for (ch <- transCh) yield transients(ch).samples.size,
          samples_val = (for {ch <- transCh.view; s <-transients(ch).samples.view} yield s.toShort).toArrayVec
        )
      }
    }
  }

}
