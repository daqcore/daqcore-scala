/*
 * Automatically generated by jrpcgen 1.0.7 on 22.12.09 14:59
 * jrpcgen is part of the "Remote Tea" ONC/RPC package for Java
 * See http://remotetea.sourceforge.net for details
 */
package org.daqcore.oncrpc.vxi11core;
import org.acplt.oncrpc.*;
import java.io.IOException;

public class Device_ErrorCode implements XdrAble, java.io.Serializable {

    public int value;

    private static final long serialVersionUID = -4013417079871585591L;

    public Device_ErrorCode() {
    }

    public Device_ErrorCode(int value) {
        this.value = value;
    }

    public Device_ErrorCode(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        xdrDecode(xdr);
    }

    public void xdrEncode(XdrEncodingStream xdr)
           throws OncRpcException, IOException {
        xdr.xdrEncodeInt(value);
    }

    public void xdrDecode(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        value = xdr.xdrDecodeInt();
    }

}
// End of Device_ErrorCode.java