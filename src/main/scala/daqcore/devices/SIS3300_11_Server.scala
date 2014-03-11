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
class SIS3300_11_Server(vmeBus: VMEBus, baseAddress: Int, devId: Int = -1) extends SIS3300Server(vmeBus, baseAddress, devId) {
  import SIS3300._
  import SIS3300Server._
  import SIS3300_11_Server._

  override def profiles = super.profiles.+[SIS3300_11]

  val memory = new SISMemory11(vmeBus, baseAddress, vmeBus.defaultTimeout)


  def setTrigThresh(thresholds: (Int, TriggerThreshold)*) {
    import memory._
    
    val firLen = settings.trigger.mode.asInstanceOf[FIRTriggerMode].nPeak
    val firSumShift = math.max(floorLog2(firLen) - 2, 0)
    def firThresh(thresh: Int) = ((thresh * firLen) >> firSumShift) + 0x8000

    val threshMap =
      SortedMap(channels map {ch => ch -> TrigOff}: _*) ++
        (settings.trigger.thresholds) ++
        SortedMap(thresholds: _*)
    
    val cfg = threshMap map { e =>
      val (ch, TriggerThreshold(threshold, pol)) = e
      ch -> TriggerThreshold(
        findNearestInt(-0x1000 to 0xfff, threshold),
        pol
      )
    }
    
    debug("Setting: " + cfg)

    run { for {
      _ <- TRIGGER_THRESHOLD_ADC12.THRESHODD set firThresh(cfg(1).threshold)
      _ <- TRIGGER_SETUP_ADC12.LTODD set (if (cfg(1).polarity) 0 else 1)
      _ <- TRIGGER_SETUP_ADC12.GTODD set (if (cfg(1).polarity) 1 else 0)
      _ <- TRIGGER_THRESHOLD_ADC12.THRESHEVEN set firThresh(cfg(2).threshold)
      _ <- TRIGGER_SETUP_ADC12.LTEVEN set (if (cfg(2).polarity) 0 else 1)
      _ <- TRIGGER_SETUP_ADC12.GTEVEN set (if (cfg(2).polarity) 1 else 0)

      _ <- TRIGGER_THRESHOLD_ADC34.THRESHODD set firThresh(cfg(3).threshold)
      _ <- TRIGGER_SETUP_ADC34.LTODD set (if (cfg(3).polarity) 0 else 1)
      _ <- TRIGGER_SETUP_ADC34.GTODD set (if (cfg(3).polarity) 1 else 0)
      _ <- TRIGGER_THRESHOLD_ADC34.THRESHEVEN set firThresh(cfg(4).threshold)
      _ <- TRIGGER_SETUP_ADC34.LTEVEN set (if (cfg(4).polarity) 0 else 1)
      _ <- TRIGGER_SETUP_ADC34.GTEVEN set (if (cfg(4).polarity) 1 else 0)

      _ <- TRIGGER_THRESHOLD_ADC56.THRESHODD set firThresh(cfg(5).threshold)
      _ <- TRIGGER_SETUP_ADC56.LTODD set (if (cfg(5).polarity) 0 else 1)
      _ <- TRIGGER_SETUP_ADC56.GTODD set (if (cfg(5).polarity) 1 else 0)
      _ <- TRIGGER_THRESHOLD_ADC56.THRESHEVEN set firThresh(cfg(6).threshold)
      _ <- TRIGGER_SETUP_ADC56.LTEVEN set (if (cfg(6).polarity) 0 else 1)
      _ <- TRIGGER_SETUP_ADC56.GTEVEN set (if (cfg(6).polarity) 1 else 0)

      _ <- TRIGGER_THRESHOLD_ADC78.THRESHODD set firThresh(cfg(7).threshold)
      _ <- TRIGGER_SETUP_ADC78.LTODD set (if (cfg(7).polarity) 0 else 1)
      _ <- TRIGGER_SETUP_ADC78.GTODD set (if (cfg(7).polarity) 1 else 0)
      _ <- TRIGGER_THRESHOLD_ADC78.THRESHEVEN set firThresh(cfg(8).threshold)
      _ <- TRIGGER_SETUP_ADC78.LTEVEN set (if (cfg(8).polarity) 0 else 1)
      _ <- TRIGGER_SETUP_ADC78.GTEVEN set (if (cfg(8).polarity) 1 else 0)

      _ <- sync()
    } yield {} }
    
    settingsVar = settingsVar.copy (
      trigger = settingsVar.trigger.copy (
        thresholds = cfg
      )
    )
  }


  def setFIRTrigMode(toSet: FIRTriggerMode) {
    import memory._

    val clampedSettings = {
      import toSet._
      FIRTriggerMode (
        nPeak = findNearestInt((2 to 0xff), nPeak),
        nGap = findNearestInt((2 to 0xff), nGap),
        p = findNearestInt((0 to 0xf), p),
        test = findNearestInt((0 to 2), test)
      )
    }

    debug("Setting: " + clampedSettings)
  
    for (reg <- Seq(TRIGGER_SETUP_ADC12, TRIGGER_SETUP_ADC34, TRIGGER_SETUP_ADC56, TRIGGER_SETUP_ADC78)) {
      import clampedSettings._
      run { for {
        _ <- reg.FIRON set 0
        _ <- sync()
        _ <- reg.NPEAK set nPeak
        _ <- reg.NGAP set nGap
        _ <- reg.FIRTEST set (if (test > 0) 1 else 0)
        _ <- reg.TESTEVEN set (if (test == 2) 1 else 0)
        _ <- reg.NPULSE set p
        _ <- reg.PULSE set (if (p > 0) 1 else 0)
        _ <- sync()
        _ <- reg.FIRON set 1
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
    case mode: FIRTriggerMode => setFIRTrigMode(mode)
    case mode => throw new IllegalArgumentException("Trigger mode %s not supported".format(mode))
  }
}


object SIS3300_11_Server {  
  import SIS3300Server._

  def apply(vmeBus: VMEBus, baseAddress: Int, devId: Int = -1, sv: Supervising = defaultSupervisor, lc: LifeCycle = UndefinedLifeCycle): SIS3300_11 =
    new ServerProxy(sv.linkStart(actorOf(new SIS3300_11_Server(vmeBus, baseAddress, devId)), lc)) with SIS3300_11

  class SISMemory11(mem: VMEBus, base: Address, timeout: Long = 10000) extends SISMemory(mem, base, timeout) {
    val majorFirmwareRevision = 0x11
  
    // /** Trigger Threshold Register, all ADCs (0x100004, write-only) */
    // val THRESHOLD_ALL_ADC = new WORegister(0x100004) ...

    /** Trigger Threshold Register, ADC group specific (read/write) */
    class TriggerThresholdRegister(addr: Address) extends RWRegister(addr) {
      /** Threshold value, even-numbered ADCs (2/4/6/8) */
      def THRESHEVEN = RWBitRange(0, 15)
      /** GT or LE, even-numbered ADCs (0: GT, 1: LE) */
      def THRESHODD = RWBitRange(16, 31)
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
     /** Peaking time P */
      def NPEAK = RWBitRange(0, 7)
      /** Gap time G */
      def NGAP = RWBitRange(8, 15)
      /** Pulse length P */
      def NPULSE = RWBitRange(16, 19)
      /** Test even/odd ADC channel in FIR test mode (0: odd ADC FIR -> even ADC mem, 1: even -> odd) */
      def TESTEVEN = RWBit(20)
      /** Enable FIR test mode */
      def FIRTEST = RWBit(21)
      /** Trigger mode of odd ADC LT */
      def LTODD = RWBit(24)
      /** Trigger mode of odd ADC GT */
      def GTODD = RWBit(25)
      /** Trigger mode of even ADC LT */
      def LTEVEN = RWBit(26)
      /** Trigger mode of even ADC GT */
      def GTEVEN = RWBit(27)
      /** Enable pulse mode */
      def PULSE = RWBit(28)
      /** Enable FIR trigger */
      def FIRON = RWBit(31)
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
