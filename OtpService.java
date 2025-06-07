import com.eatthepath.otp.TimeBasedOneTimePasswordGenerator;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;

public class OtpService {

    private static final Duration OTP_TIME_STEP = Duration.ofSeconds(30); // each step = 30 sec
    private static final int VALIDITY_SECONDS = 90; // OTP valid for 90 seconds

    private final TimeBasedOneTimePasswordGenerator totpGenerator;

    public OtpService() throws NoSuchAlgorithmException {
        this.totpGenerator = new TimeBasedOneTimePasswordGenerator(OTP_TIME_STEP);
    }

    public String generateOtp(String base64SecretKey) throws Exception {
        SecretKey secretKey = decodeBase64Key(base64SecretKey);
        Instant now = Instant.now();
        return String.format("%06d", totpGenerator.generateOneTimePassword(secretKey, now));
    }

    private SecretKey decodeBase64Key(String base64) {
        byte[] decodedKey = Base64.getDecoder().decode(base64);
        return new javax.crypto.spec.SecretKeySpec(decodedKey, "HmacSHA1");
    }

    public boolean validateOtp(String otp, String base64SecretKey) throws Exception {
        SecretKey secretKey = decodeBase64Key(base64SecretKey);
        Instant now = Instant.now();

        // Acceptable window = VALIDITY_SECONDS / OTP_TIME_STEP
        int steps = VALIDITY_SECONDS / (int) OTP_TIME_STEP.getSeconds();

        for (int i = -steps / 2; i <= steps / 2; i++) {
            Instant comparisonTime = now.plusSeconds(i * OTP_TIME_STEP.getSeconds());
            String candidateOtp = String.format("%06d", totpGenerator.generateOneTimePassword(secretKey, comparisonTime));
            if (candidateOtp.equals(otp)) {
                return true;
            }
        }

        return false;
    }

    // To generate a new secret key (on server side)
    public static String generateBase64SecretKey() throws NoSuchAlgorithmException {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("HmacSHA1");
        keyGenerator.init(160); // RFC recommends at least 160 bits for SHA-1
        SecretKey key = keyGenerator.generateKey();
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }
}