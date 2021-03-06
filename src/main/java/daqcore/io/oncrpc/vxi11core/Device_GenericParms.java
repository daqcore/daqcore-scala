/*
 * Automatically generated by jrpcgen 1.0.7 on 19.05.11 01:50
 * jrpcgen is part of the "Remote Tea" ONC/RPC package for Java
 * See http://remotetea.sourceforge.net for details
 */
package daqcore.io.oncrpc.vxi11core;
import org.acplt.oncrpc.*;
import java.io.IOException;

public class Device_GenericParms implements XdrAble, java.io.Serializable {
    public Device_Link lid;
    public Device_Flags flags;
    public int lock_timeout;
    public int io_timeout;

    private static final long serialVersionUID = -700907021475904131L;

    public Device_GenericParms() {
    }

    public Device_GenericParms(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        xdrDecode(xdr);
    }

    public void xdrEncode(XdrEncodingStream xdr)
           throws OncRpcException, IOException {
        lid.xdrEncode(xdr);
        flags.xdrEncode(xdr);
        xdr.xdrEncodeInt(lock_timeout);
        xdr.xdrEncodeInt(io_timeout);
    }

    public void xdrDecode(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        lid = new Device_Link(xdr);
        flags = new Device_Flags(xdr);
        lock_timeout = xdr.xdrDecodeInt();
        io_timeout = xdr.xdrDecodeInt();
    }

}
// End of Device_GenericParms.java
