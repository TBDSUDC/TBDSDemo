package com.tencent.bigdata.hbase;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.util.Random;
import org.apache.commons.codec.digest.HmacUtils;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.commons.codec.binary.Base64;

/**
 * See the instructions under hbase-examples/README.txt
 */
public class ThriftRpcHttpDoAsClient {

  static protected int port;
  static protected String host;
  CharsetDecoder decoder = null;
  static protected String doAsUser = null;
  static protected String principal = null;
  private String secureId;
  private String secureKey;

  public static void main(String[] args) throws Exception {

    if (args.length != 5) {

      System.out.println("Invalid arguments!");
      System.out.println("Usage: HttpDoAsClient host port doAsUserName secureId secureKey");
      System.exit(-1);
    }

    host = args[0];
    port = Integer.parseInt(args[1]);

    doAsUser = args[2];

    final ThriftRpcHttpDoAsClient client = new ThriftRpcHttpDoAsClient();
    client.secureId = args[3];
    client.secureKey = args[4];
    client.run();
  }

  // Helper to translate byte[]'s to UTF8 strings
  private String utf8(byte[] buf) {
    try {
      return Charset.forName("UTF-8").newDecoder().decode(ByteBuffer.wrap(buf)).toString();
    } catch (CharacterCodingException e) {
      return "[INVALID UTF-8]";
    }
  }

  private void run() throws Exception {
    TTransport transport = new TSocket(host, port);

    transport.open();
    String url = "http://" + host + ":" + port;
    THttpClient httpClient = new THttpClient(url);
    httpClient.open();
    TProtocol protocol = new TBinaryProtocol(httpClient);
    Hbase.Client client = new Hbase.Client(protocol);

    // Scan all tables, look for the demo table and delete it.
    System.out.println("scanning tables...");
    List<ByteBuffer> tableNames = refresh(client, httpClient).getTableNames();
    for (ByteBuffer name : tableNames) {
      System.out.println("  found: " + utf8(name.array()));
    }

    transport.close();
    httpClient.close();
  }
  
  /**
   * 
   * @param secureId  
   * @param timestamp   从1970到现在的秒数，java可使用System.currentTimeMillis() 获取
   * @param randomValue 任意随机数，java可使用new Random(timestamp).nextInt(Integer.MAX_VALUE)获取
   * @param secureKey
   * @return
   */
  public static String generateSignature(String secureId, long timestamp, int randomValue,
      String secureKey) {
    Base64 base64 = new Base64();
    byte[] baseStr =
        base64.encode(HmacUtils.hmacSha1(secureKey, secureId + timestamp + randomValue));

    String result = "";
    try {
      result = URLEncoder.encode(new String(baseStr), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }

    return result;
  }

  /**
   * 在httpClient请求上增加header:doAs 和 tbds-auth。 其中doAs是实际操作用户的用户名 tbds-auth用于认证，如果不设置doAs
   * header，那么运行用户和tbd-auth所设置的secureId关联的用户相同
   * 
   * @param client
   * @param httpClient
   * @return
   */
  private Hbase.Client refresh(Hbase.Client client, THttpClient httpClient) {
    httpClient.setCustomHeader("doAs", doAsUser);

    // 获取当前时间，从1970年1月1号到现在的毫秒数（UTC时间）
    long timestamp = System.currentTimeMillis();

    // 生成随机正正数
    int nonce = new Random(timestamp).nextInt(Integer.MAX_VALUE);

    // 生成签名
    String signature = generateSignature(secureId, timestamp, nonce, secureKey);

    httpClient.setCustomHeader("tbds-auth",
        String.format("%s %d %d %s", secureId, timestamp, nonce, signature));
    return client;
  }
}
