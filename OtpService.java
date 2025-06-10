import com.eatthepath.otp.TimeBasedOneTimePasswordGenerator;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

public class OtpService {

    private static final int OTP_DIGITS = 6;
    private static final long TIME_STEP_SECONDS = 30;
    private static final long VALIDITY_SECONDS = 90;

    private final TimeBasedOneTimePasswordGenerator totpGenerator;

    public OtpService() throws NoSuchAlgorithmException {
        this.totpGenerator = new TimeBasedOneTimePasswordGenerator(TIME_STEP_SECONDS, TimeUnit.SECONDS);
    }

    // Generate a new base64 encoded secret key (called only on OTP generation)
    public static String generateSecretKey() throws NoSuchAlgorithmException {
        KeyGenerator keyGen = KeyGenerator.getInstance("HmacSHA1");
        keyGen.init(160); // 160 bits for SHA1
        SecretKey key = keyGen.generateKey();
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }

    // Generate OTP based on secret
    public String generateOtp(String base64Secret) throws Exception {
        SecretKey key = decodeBase64Key(base64Secret);
        long counter = getCurrentCounter();
        int otp = totpGenerator.generateOneTimePassword(key, counter);
        return String.format("%0" + OTP_DIGITS + "d", otp);
    }

    // Validate OTP with ± (VALIDITY_SECONDS / TIME_STEP_SECONDS) / 2 window
    public boolean validateOtp(String base64Secret, String inputOtp) throws Exception {
        SecretKey key = decodeBase64Key(base64Secret);
        long currentCounter = getCurrentCounter();
        int window = (int)(VALIDITY_SECONDS / TIME_STEP_SECONDS) / 2;

        for (int i = -window; i <= window; i++) {
            long counterToTry = currentCounter + i;
            int candidate = totpGenerator.generateOneTimePassword(key, counterToTry);
            String formatted = String.format("%0" + OTP_DIGITS + "d", candidate);
            if (formatted.equals(inputOtp)) {
                return true;
            }
        }
        return false;
    }

    private long getCurrentCounter() {
        return Instant.now().getEpochSecond() / TIME_STEP_SECONDS;
    }

    private SecretKey decodeBase64Key(String base64Key) {
        byte[] decoded = Base64.getDecoder().decode(base64Key);
        return new SecretKeySpec(decoded, "HmacSHA1");
    }
}