package com.tencent.bigdata.hbase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Random;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.HmacUtils;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * See the instructions under hbase-examples/README.txt
 */
public class ThriftRpcClient {
  static protected int port;
  static protected String host;
  CharsetDecoder decoder = null;

  public static void main(String[] args) throws Exception {
    System.out.println("nonce:" + Integer.MAX_VALUE);
    if (args.length != 4) {
      System.out.println("Invalid arguments!");
      System.out.println("Usage: DemoClient host port secureId secureKey ");

      System.exit(-1);
    }

    port = Integer.parseInt(args[1]);
    host = args[0];

    final ThriftRpcClient client = new ThriftRpcClient();
    client.run(args[2], args[3]);
  }

  // Helper to translate byte[]'s to UTF8 strings
  private String utf8(byte[] buf) {
    try {
      return Charset.forName("UTF-8").newDecoder().decode(ByteBuffer.wrap(buf)).toString();
    } catch (CharacterCodingException e) {
      return "[INVALID UTF-8]";
    }
  }

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

  private String getAuthentication(String secureId, String secureKey) {
    // 获取当前时间，从1970年1月1号到现在的毫秒数（UTC时间）
    long timestamp = System.currentTimeMillis();

    // 生成随机正正数
    int nonce = new Random(timestamp).nextInt(Integer.MAX_VALUE);

    // 生成签名
    String signature = generateSignature(secureId, timestamp, nonce, secureKey);
    System.out.println("auth:"
        + String.format("get authentication:%s %d %d %s", secureId, timestamp, nonce, signature));

    return String.format("%s %d %d %s", secureId, timestamp, nonce, signature);
  }

  private void run(String secureId, String secureKey) throws Exception {
    TTransport transport = new TSocket(host, port);

    // mechanism必须设置为 PLAIN
    transport = new TSaslClientTransport("PLAIN", null, null, null, null, new CallbackHandler() {
      @Override
      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
          if (callback instanceof NameCallback) {
            // 在NameCallBack里面设置认证字符串，格式必须是各个参数以空格隔开，参数顺序依次是：secureId timestamp nonce signature
            // 具体格式见getAuthentication函数返回的格式
            ((NameCallback) callback).setName(getAuthentication(secureId, secureKey));
          } else if (callback instanceof PasswordCallback) {
            // PasswordCallback设置为空即可，服务端认证时不用
            ((PasswordCallback) callback).setPassword("".toCharArray());;
          } else if (callback instanceof AuthorizeCallback) {
            // AuthorizeCallback设置为空即可，服务端认证时不用
            ((AuthorizeCallback) callback).setAuthorizedID("");
          } else {
            throw new UnsupportedCallbackException(callback);
          }
        }
      }
    }, transport);

    transport.open();

    TProtocol protocol = new TBinaryProtocol(transport, true, true);
    Hbase.Client client = new Hbase.Client(protocol);

    System.out.println("scanning tables...");
    for (ByteBuffer name : client.getTableNames()) {
      System.out.println("  found: " + utf8(name.array()));
    }
    
    transport.close();
  }
}
