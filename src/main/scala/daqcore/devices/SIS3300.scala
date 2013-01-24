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

import akka.dispatch.Future
import akka.config.Supervision.{LifeCycle, UndefinedLifeCycle}

import daqcore.util._
import daqcore.actors._
import daqcore.io._


trait SIS3300 extends EventSource with Device {
  import SIS3300._

  def resetModule() = srv ! ResetModule()
  def initModule() = srv ! InitModule()

  def getModuleInfo(): Future[ModuleInfo] = srv !!> GetModuleInfo()
  
  def setUserLED(state: Boolean) = srv ! SetUserLED(state)
  def getUserLED(): Future[Boolean] = srv !!> GetUserLED()

  def setUserOutput(state: Boolean) = srv ! SetUserOutput(state)
  def getUserOutput(): Future[Boolean] = srv !!> GetUserOutput()

  def setupIRQ(level: Int, vector: Int) = srv ! SetupIRQ(level, vector)
  def clearIRQ() = srv ! ClearIRQ()
  def getIRQStatus(): Future[Boolean] = srv !!> GetIRQStatus()

  def getSettings(): Future[Settings] = srv !!> GetSettings()

  def setDAQSettings(toSet: DAQSettings) = srv ! SetDAQSettings(toSet)
  def setTrigMode(toSet: TriggerMode) = srv ! SetTrigMode(toSet)
  def setTrigThresh(thresholds: (Int, TriggerThreshold)*) =
    srv ! SetTrigThresh(thresholds: _*)

  def setTimeStampMode(mode: TimeStampMode) = srv ! SetTimeStampMode(mode)
  def getTimeStampMode() = srv !!> GetTimeStampMode()

  def startCapture() = srv ! StartCapture()
  def stopCapture() = srv ! StopCapture()

  def getBankBusy(): Future[Boolean] = srv !!> GetBankBusy()
  def getBankFull(): Future[Boolean] =  srv !!> GetBankFull()
  def getNEvents(): Future[Int] =  srv !!> GetNEvents()
  def getBankStates(): Future[BankStates] =  srv !!> GetBankStates()
}


object SIS3300 {
  case class ModuleInfo(modID: String, fwRevMajor: Int, fwRevMinor: Int)

  case class Settings (
    daq: DAQSettings = DAQSettings(),
    trigger: TriggerSettings = TriggerSettings (
      thresholds = SortedMap.empty[Int, TriggerThreshold],
      mode = MNPTriggerMode()
    )
  )

  case class DAQSettings (
    nSamples: Int = 4096,
    stopDelay: Int = 2048,
    nAverage: Int = 1,
    sampleRate: Double = 100E6,
    tsBase: Double = 10E-9.toLong, // in ns
    nPages: Int = Int.MaxValue,
    trigOnly: Boolean = false,
    captureUserIn: Boolean = false
  )

  case class TriggerSettings (
    thresholds: SortedMap[Int, TriggerThreshold],
    mode: TriggerMode
  )
  
  case class TriggerThreshold (
    threshold: Int = 0,
    polarity: Boolean = true
  )
  val TrigOff = TriggerThreshold(threshold = 0xfff, polarity = true)
  
  abstract class TriggerMode
  case class MNPTriggerMode (m: Int = 0x8, n: Int = 0x8, p: Int = 0x8) extends TriggerMode
  case class FIRTriggerMode (nGap: Int = 0x20, nPeak: Int = 0x20, p: Int = 0x8, test: Int = 0) extends TriggerMode

  case class BankState(nevents: Int, busy: Boolean, full: Boolean)
  case class BankStates(currentBank: Int, states: Map[Int, BankState])

  abstract class TimeStampMode
  case object TSClearOnBankSwitch extends TimeStampMode
  case object TSClearNever extends TimeStampMode
  case object TSClearOnFirstStop extends TimeStampMode
  case object TSClearOnExternal extends TimeStampMode
  
  case class ResetModule() extends ActorCmd
  case class InitModule() extends ActorCmd

  case class GetModuleInfo() extends ActorQuery[ModuleInfo]

  case class SetUserLED(state: Boolean) extends ActorCmd
  case class GetUserLED() extends ActorQuery[Boolean]

  case class SetUserOutput(state: Boolean) extends ActorCmd
  case class GetUserOutput() extends ActorQuery[Boolean]

  case class SetupIRQ(level: Int, vector: Int) extends ActorCmd
  case class ClearIRQ() extends ActorCmd
  case class GetIRQStatus() extends ActorQuery[Boolean]

  case class GetSettings() extends ActorQuery[Settings]

  case class SetDAQSettings(toSet: DAQSettings) extends ActorCmd
  case class SetTrigMode(toSet: TriggerMode)
  case class SetTrigThresh(thresholds: (Int, TriggerThreshold)*) extends ActorCmd

  case class SetTimeStampMode(mode: TimeStampMode) extends ActorCmd // Only supported by some firmware versions
  case class GetTimeStampMode() extends ActorQuery[TimeStampMode] // Only supported by some firmware versions

  case class StartCapture() extends ActorCmd
  case class StopCapture() extends ActorCmd

  case class GetBankBusy() extends ActorQuery[Boolean]
  case class GetBankFull() extends ActorQuery[Boolean]
  case class GetNEvents() extends ActorQuery[Int]
  case class GetBankStates() extends ActorQuery[BankStates]
}



trait SIS3300_03 extends ServerProfile with SIS3300 {
}


object SIS3300_03 {
  def apply(vmeBus: VMEBus, baseAddress: Int, sv: Supervising = defaultSupervisor, lc: LifeCycle = UndefinedLifeCycle): SIS3300_03 =
    SIS3300_03_Server(vmeBus, baseAddress, sv)
}



trait SIS3300_11 extends ServerProfile with SIS3300 {
}


object SIS3300_11 {
  def apply(vmeBus: VMEBus, baseAddress: Int, sv: Supervising = defaultSupervisor, lc: LifeCycle = UndefinedLifeCycle): SIS3300_11 =
    SIS3300_11_Server(vmeBus, baseAddress, sv)
}
