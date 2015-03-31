// Copyright (C) 2015 Oliver Schulz <oliver.schulz@tu-dortmund.de>

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


package daqcore.io

import scala.language.implicitConversions


package object memory {

implicit def byteBitAccessOps(x: Byte)  = new ByteBitAccessOps(x)
implicit def shortBitAccessOps(x: Short)  = new ShortBitAccessOps(x)
implicit def intBitAccessOps(x: Int)  = new IntBitAccessOps(x)
implicit def longBitAccessOps(x: Long)  = new LongBitAccessOps(x)

implicit def byteBitSelectionOps(bitSelection: BitSelection[Byte])  = new ByteBitSelectionOps(bitSelection)
implicit def shortBitSelectionOps(bitSelection: BitSelection[Short])  = new ShortBitSelectionOps(bitSelection)
implicit def intBitSelectionOps(bitSelection: BitSelection[Int])  = new IntBitSelectionOps(bitSelection)
implicit def longBitSelectionOps(bitSelection: BitSelection[Long])  = new LongBitSelectionOps(bitSelection)

implicit def byteBitOps(bit: Bit[Byte])  = new ByteBitOps(bit)
implicit def shortBitOps(bit: Bit[Short])  = new ShortBitOps(bit)
implicit def intBitOps(bit: Bit[Int])  = new IntBitOps(bit)
implicit def longBitOps(bit: Bit[Long])  = new LongBitOps(bit)

implicit def byteBitRangeOps(bitRange: BitRange[Byte])  = new ByteBitRangeOps(bitRange)
implicit def shortBitRangeOps(bitRange: BitRange[Short])  = new ShortBitRangeOps(bitRange)
implicit def intBitRangeOps(bitRange: BitRange[Int])  = new IntBitRangeOps(bitRange)
implicit def longBitRangeOps(bitRange: BitRange[Long])  = new LongBitRangeOps(bitRange)

implicit def byteRegisterFieldsOps(fields: Register[Byte]#Fields)  = new ByteRegisterFieldsOps(fields)
implicit def shortRegisterFieldsOps(fields: Register[Short]#Fields)  = new ShortRegisterFieldsOps(fields)
implicit def intRegisterFieldsOps(fields: Register[Int]#Fields)  = new IntRegisterFieldsOps(fields)
implicit def longRegisterFieldsOps(fields: Register[Long]#Fields)  = new LongRegisterFieldsOps(fields)

}