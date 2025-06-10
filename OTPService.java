# Fixing TOTP Validity Time Inconsistency in Spring Boot

The issue you're experiencing with varying validity periods (30s, 60s, 90s) typically occurs due to improper time step handling or synchronization problems. Here's how to fix it:

## Root Cause Analysis

The inconsistency happens because:
1. The TOTP counter calculation isn't properly aligned with 90-second windows
2. System time might not be synchronized
3. The verification window isn't consistently applied

## Solution Implementation

### 1. Fix the Time Step Calculation

Modify your `TotpService` to ensure consistent 90-second windows:

```java
import java.time.Instant;

@Service
public class TotpService {
    private static final int TIME_STEP = 90; // Fixed 90-second window
    
    // Updated method to get current counter
    private long getCurrentCounter() {
        long currentTimeSeconds = Instant.now().getEpochSecond();
        return currentTimeSeconds / TIME_STEP;
    }
    
    // Update generateTotp method
    public String generateTotp(String secret) {
        return generateHotp(secret, getCurrentCounter(), CODE_DIGITS);
    }
    
    // Update verifyTotp method
    public boolean verifyTotp(String secret, String code) {
        long counter = getCurrentCounter();
        
        // Check current window
        if (generateHotp(secret, counter, CODE_DIGITS).equals(code)) {
            return true;
        }
        
        // Check previous window only (not next window)
        if (generateHotp(secret, counter - 1, CODE_DIGITS).equals(code)) {
            return true;
        }
        
        return false;
    }
}
```

### 2. Add Time Synchronization Check

```java
@Service
public class TotpService {
    // Add this method to check time synchronization
    public void checkTimeSynchronization() {
        long systemTime = Instant.now().getEpochSecond();
        long ntpTime = getNtpTime(); // Implement NTP time fetch
        
        if (Math.abs(systemTime - ntpTime) > 10) { // More than 10 seconds drift
            throw new RuntimeException("System time is not synchronized");
        }
    }
    
    // Basic NTP time implementation (for production, use a proper NTP client)
    private long getNtpTime() {
        try {
            // In production, use a proper NTP client library
            return Instant.now().getEpochSecond(); // Replace with actual NTP time
        } catch (Exception e) {
            return Instant.now().getEpochSecond();
        }
    }
}
```

### 3. Add Validity Time Calculation

```java
@Service
public class TotpService {
    // Add this method to get remaining validity time
    public int getRemainingValiditySeconds() {
        long currentTimeSeconds = Instant.now().getEpochSecond();
        return TIME_STEP - (int)(currentTimeSeconds % TIME_STEP);
    }
    
    // Update controller to show remaining time
    @GetMapping("/generate-code")
    public Map<String, Object> generateCode(@RequestParam String secret) {
        Map<String, Object> response = new HashMap<>();
        response.put("code", totpService.generateTotp(secret));
        response.put("validForSeconds", totpService.getRemainingValiditySeconds());
        return response;
    }
}
```

### 4. Add Logging for Debugging

```java
@Service
public class TotpService {
    private static final Logger logger = LoggerFactory.getLogger(TotpService.class);
    
    public boolean verifyTotp(String secret, String code) {
        long counter = getCurrentCounter();
        long currentTime = Instant.now().getEpochSecond();
        
        logger.debug("Verifying OTP at time: {}, counter: {}", currentTime, counter);
        logger.debug("Current window: {} to {}", 
            counter * TIME_STEP, 
            (counter + 1) * TIME_STEP);
        
        // Rest of verification logic...
    }
}
```

## Testing the Fix

1. **Verify Time Step Alignment**:
   - Generate an OTP and note the exact time
   - The OTP should be valid for exactly (90 - (current_time % 90)) seconds

2. **Check Synchronization**:
   - Call the `/api/otp/generate-code` endpoint
   - The `validForSeconds` value should consistently decrease from 90 down to 0

3. **Window Verification**:
   - An OTP generated at time T should be valid until T+90 seconds
   - It should not be valid at T+91 seconds

## Additional Recommendations

1. **Use NTP Synchronization**:
   ```java
   // Add to your pom.xml
   <dependency>
       <groupId>org.apache.commons</groupId>
       <artifactId>commons-net</artifactId>
       <version>3.8.0</version>
   </dependency>
   
   // Implement proper NTP time fetch
   private long getNtpTime() throws IOException {
       NTPUDPClient timeClient = new NTPUDPClient();
       InetAddress inetAddress = InetAddress.getByName("pool.ntp.org");
       TimeInfo timeInfo = timeClient.getTime(inetAddress);
       return timeInfo.getMessage().getTransmitTimeStamp().getTime() / 1000;
   }
   ```

2. **Add Integration Tests**:
   ```java
   @SpringBootTest
   public class TotpServiceTest {
       
       @Autowired
       private TotpService totpService;
       
       @Test
       public void testValidityPeriod() throws InterruptedException {
           String secret = totpService.generateSecret();
           String code = totpService.generateTotp(secret);
           
           int remaining = totpService.getRemainingValiditySeconds();
           assertTrue(remaining > 0 && remaining <= 90);
           
           // Verify immediately should work
           assertTrue(totpService.verifyTotp(secret, code));
           
           // Wait until the window expires
           Thread.sleep((remaining + 1) * 1000);
           assertFalse(totpService.verifyTotp(secret, code));
       }
   }
   ```

These changes will ensure your TOTP implementation consistently uses 90-second windows and properly reports remaining validity time. The OTPs should now always be valid for exactly 90 seconds from their generation time.