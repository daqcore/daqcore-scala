/*
 * Automatically generated by jrpcgen 1.0.7 on 19.05.11 01:50
 * jrpcgen is part of the "Remote Tea" ONC/RPC package for Java
 * See http://remotetea.sourceforge.net for details
 */
package daqcore.io.oncrpc.vxi11core;
import org.acplt.oncrpc.*;
import java.io.IOException;

public class Create_LinkResp implements XdrAble, java.io.Serializable {
    public Device_ErrorCode error;
    public Device_Link lid;
    public short abortPort;
    public int maxRecvSize;

    private static final long serialVersionUID = -5708438598941887891L;

    public Create_LinkResp() {
    }

    public Create_LinkResp(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        xdrDecode(xdr);
    }

    public void xdrEncode(XdrEncodingStream xdr)
           throws OncRpcException, IOException {
        error.xdrEncode(xdr);
        lid.xdrEncode(xdr);
        xdr.xdrEncodeShort(abortPort);
        xdr.xdrEncodeInt(maxRecvSize);
    }

    public void xdrDecode(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        error = new Device_ErrorCode(xdr);
        lid = new Device_Link(xdr);
        abortPort = xdr.xdrDecodeShort();
        maxRecvSize = xdr.xdrDecodeInt();
    }

}
// End of Create_LinkResp.java