package com.mikeal.kafkatest;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.HmacUtils;

public class TbdsAuthenticationUtil {

  public static final String KAFKA_SECURITY_PROTOCOL = "security.protocol";
  public static final String KAFKA_SASL_MECHANISM = "sasl.mechanism";
  public static final String KAFKA_SECURITY_PROTOCOL_AVLUE = "SASL_TBDS";
  public static final String KAFKA_SASL_MECHANISM_VALUE = "TBDS";

  public static final String KAFKA_SASL_TBDS_SECURE_ID = "sasl.tbds.secure.id";
  public static final String KAFKA_SASL_TBDS_SECURE_KEY = "sasl.tbds.secure.key";
  
  public static String generateSignature(String secureId, long timestamp, int randomValue, String secureKey){
    Base64 base64 = new Base64();
    byte[] baseStr = base64.encode(HmacUtils.hmacSha1(secureKey, secureId + timestamp + randomValue));
    
    String result = "";
    try {
      result = URLEncoder.encode(new String(baseStr), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    
    return  result;
  }
}
