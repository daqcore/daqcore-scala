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


package daqcore.devices

import scala.collection.immutable.{IntMap, SortedMap}
import scala.collection.MapLike

import akka.actor._, akka.actor.Actor._
import akka.config.Supervision.{LifeCycle, UndefinedLifeCycle, Temporary}

import daqcore.util._
import daqcore.actors._
import daqcore.io._
import daqcore.monads._


// SIS3300, default Firmware, major version 03
class SIS3300_03_Server(vmeBus: VMEBus, baseAddress: Int, devId: Int = -1) extends SIS3300Server(vmeBus, baseAddress, devId) {
  import SIS3300._
  import SIS3300Server._
  import SIS3300_03_Server._

  override def profiles = super.profiles.+[SIS3300_03]

  val memory = new SISMemory03(vmeBus, baseAddress, vmeBus.defaultTimeout)


  def setTrigThresh(thresholds: (Int, TriggerThreshold)*) {
    import memory._

    val threshMap =
      SortedMap(channels map {ch => ch -> TrigOff}: _*) ++
        (settings.trigger.thresholds) ++
        SortedMap(thresholds: _*)
    
    val cfg = threshMap map { e =>
      val (ch, TriggerThreshold(threshold, pol)) = e
      ch -> TriggerThreshold(
        findNearestInt(sampleRange, threshold),
        pol
      )
    }
    
    debug("Setting: " + cfg)

    run { for {
      _ <- TRIGGER_THRESHOLD_ADC12.THRESHODD set cfg(1).threshold
      _ <- TRIGGER_THRESHOLD_ADC12.GTLEODD set (if (cfg(1).polarity) 0 else 1)
      _ <- TRIGGER_THRESHOLD_ADC12.THRESHEVEN set cfg(2).threshold
      _ <- TRIGGER_THRESHOLD_ADC12.GTLEEVEN set (if (cfg(2).polarity) 0 else 1)

      _ <- TRIGGER_THRESHOLD_ADC34.THRESHODD set cfg(3).threshold
      _ <- TRIGGER_THRESHOLD_ADC34.GTLEODD set (if (cfg(3).polarity) 0 else 1)
      _ <- TRIGGER_THRESHOLD_ADC34.THRESHEVEN set cfg(4).threshold
      _ <- TRIGGER_THRESHOLD_ADC34.GTLEEVEN set (if (cfg(4).polarity) 0 else 1)

      _ <- TRIGGER_THRESHOLD_ADC56.THRESHODD set cfg(5).threshold
      _ <- TRIGGER_THRESHOLD_ADC56.GTLEODD set (if (cfg(5).polarity) 0 else 1)
      _ <- TRIGGER_THRESHOLD_ADC56.THRESHEVEN set cfg(6).threshold
      _ <- TRIGGER_THRESHOLD_ADC56.GTLEEVEN set (if (cfg(6).polarity) 0 else 1)

      _ <- TRIGGER_THRESHOLD_ADC78.THRESHODD set cfg(7).threshold
      _ <- TRIGGER_THRESHOLD_ADC78.GTLEODD set (if (cfg(7).polarity) 0 else 1)
      _ <- TRIGGER_THRESHOLD_ADC78.THRESHEVEN set cfg(8).threshold
      _ <- TRIGGER_THRESHOLD_ADC78.GTLEEVEN set (if (cfg(8).polarity) 0 else 1)
      _ <- sync()
    } yield {} }
    
    settingsVar = settingsVar.copy (
      trigger = settingsVar.trigger.copy (
        thresholds = cfg
      )
    )
  }


  def setMNPTrigMode(toSet: MNPTriggerMode) {
    import memory._

    val clampedSettings = {
      import toSet._
      MNPTriggerMode (
        m = findNearestInt((0 to 0xf), m),
        n = findNearestInt((0 to 0xf), n),
        p = findNearestInt((0 to 0xf), p)
      )
    }

    debug("Setting: " + clampedSettings)
  
    for (reg <- Seq(TRIGGER_SETUP_ADC12, TRIGGER_SETUP_ADC34, TRIGGER_SETUP_ADC56, TRIGGER_SETUP_ADC78)) {
      import clampedSettings._
      run { for {
        _ <- reg.M set m
        _ <- reg.N set n
        _ <- reg.P set p
        _ <- reg.NM set 1
        _ <- reg.PULSE set (if (p > 0) 1 else 0)
        _ <- sync()
      } yield {} }
    }

    settingsVar = settingsVar.copy (
      trigger = settingsVar.trigger.copy (
        mode = clampedSettings
      )
    )
  }
  
  def srvSetTrigMode(toSet: TriggerMode) = toSet match {
    case mode: MNPTriggerMode => setMNPTrigMode(mode)
    case mode => throw new IllegalArgumentException("Trigger mode %s not supported".format(mode))
  }
}


object SIS3300_03_Server {
  import SIS3300Server._

  def apply(vmeBus: VMEBus, baseAddress: Int, devId: Int = -1, sv: Supervising = defaultSupervisor, lc: LifeCycle = UndefinedLifeCycle): SIS3300_03 =
    new ServerProxy(sv.linkStart(actorOf(new SIS3300_03_Server(vmeBus, baseAddress, devId)), lc)) with SIS3300_03

  class SISMemory03(mem: VMEBus, base: Address, timeout: Long = 10000) extends SISMemory(mem, base, timeout) {
    val majorFirmwareRevision = 0x03
  
    // /** Trigger Threshold Register, all ADCs (0x100004, write-only) */
    // val THRESHOLD_ALL_ADC = new WORegister(0x100004) ...

    /** Trigger Threshold Register, ADC group specific (read/write) */
    class TriggerThresholdRegister(addr: Address) extends RWRegister(addr) {
      /** Threshold value, even-numbered ADCs (2/4/6/8) */
      def THRESHEVEN = RWBitRange(0, 11)
      /** GT or LE, even-numbered ADCs (0: GT, 1: LE) */
      def GTLEEVEN = RWBit(15)
      /** Threshold value, odd-numbered ADCs (1/3/5/7) */
      def THRESHODD = RWBitRange(16, 27)
      /** GT or LE, odd-numbered ADCs (0: GT, 1: LE) */
      def GTLEODD = RWBit(31)
    }

    /** Trigger Threshold Register, ADC group 1/2 (0x200004, read/write) */
    val TRIGGER_THRESHOLD_ADC12 = new TriggerThresholdRegister(0x200004)
    /** Trigger Threshold Register, ADC group 3/4 (0x280004, read/write) */
    val TRIGGER_THRESHOLD_ADC34 = new TriggerThresholdRegister(0x280004)
    /** Trigger Threshold Register, ADC group 5/6 (0x300004, read/write) */
    val TRIGGER_THRESHOLD_ADC56 = new TriggerThresholdRegister(0x300004)
    /** Trigger Threshold Register, ADC group 7/8 (0x380004, read/write) */
    val TRIGGER_THRESHOLD_ADC78 = new TriggerThresholdRegister(0x380004)

  
    // /** Trigger setup register, all ADCs (0x100028, write-only) */
    // val TRIGGER_SETUP_ALL_ADC = new WORegister(0x100028) ...

    /** Trigger setup register, ADC group specific (read/write) */
    class TriggerSetupRegister(addr: Address) extends RWRegister(addr) {
     /** M */
      def M = RWBitRange(0, 3)
      /** P */
      def N = RWBitRange(8, 11)
      /** P */
      def P = RWBitRange(16, 19)
      /** enable N M mode */
      def NM = RWBit(24)
      /** enable pulse mode */
      def PULSE = RWBit(28)
    }

    /** Trigger setup register, ADC group 1/2 (0x200028, read/write) */
    val TRIGGER_SETUP_ADC12 = new TriggerSetupRegister(0x200028)
    /** Trigger setup register, ADC group 3/4 (0x280028, read/write) */
    val TRIGGER_SETUP_ADC34 = new TriggerSetupRegister(0x280028)
    /** Trigger setup register, ADC group 5/6 (0x300028, read/write) */
    val TRIGGER_SETUP_ADC56 = new TriggerSetupRegister(0x300028)
    /** Trigger setup register, ADC group 7/8 (0x380028, read/write) */
    val TRIGGER_SETUP_ADC78 = new TriggerSetupRegister(0x380028)
  }
}
