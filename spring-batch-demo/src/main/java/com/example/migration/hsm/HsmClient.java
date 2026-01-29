
package com.example.migration.hsm;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
@Component
public class HsmClient {
  private final RestTemplate restTemplate=new RestTemplate();
  public String decrypt(byte[] encrypted){
    return restTemplate.postForObject("http://internal-hsm-service/decrypt", encrypted, String.class);
  }
  public byte[] encrypt(String plain){
    return restTemplate.postForObject("http://internal-hsm-service/encrypt", plain, byte[].class);
  }
}
