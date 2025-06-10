import com.eatthepath.otp.TimeBasedOneTimePasswordGenerator;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;

public class OTPGeneratorAndValidator {

    private static final Duration TIME_STEP = Duration.ofSeconds(90); // 90-second OTP
    private static final String ALGORITHM = "HmacSHA256";

    public static void main(String[] args) throws Exception {
        // 1. Initialize the TOTP generator
        TimeBasedOneTimePasswordGenerator totp = new TimeBasedOneTimePasswordGenerator(TIME_STEP, 6, ALGORITHM);

        // 2. Generate a shared secret key
        SecretKey secretKey = generateSecretKey(totp.getAlgorithm());

        // 3. Simulate OTP generation
        Instant generationTime = Instant.now();
        int generatedOtp = totp.generateOneTimePassword(secretKey, generationTime);

        System.out.println("=== OTP Generation ===");
        System.out.println("Time Now (epoch): " + generationTime.getEpochSecond());
        System.out.println("Time Step Index:   " + (generationTime.getEpochSecond() / TIME_STEP.getSeconds()));
        System.out.println("Generated OTP:     " + generatedOtp);
        System.out.println();

        // Simulate wait or user entry delay (OPTIONAL)
        // Thread.sleep(91000); // Uncomment to test expiry after 91 seconds

        // 4. Simulate OTP validation
        Instant validationTime = Instant.now(); // Could be later than generation
        boolean isValid = validateOtp(totp, secretKey, generatedOtp, validationTime);

        System.out.println("=== OTP Validation ===");
        System.out.println("Time Now (epoch): " + validationTime.getEpochSecond());
        System.out.println("Time Step Index:   " + (validationTime.getEpochSecond() / TIME_STEP.getSeconds()));
        System.out.println("Is OTP valid?      " + isValid);
    }

    private static SecretKey generateSecretKey(String algorithm) throws NoSuchAlgorithmException {
        KeyGenerator keyGenerator = KeyGenerator.getInstance(algorithm);
        keyGenerator.init(256); // 256 bits for SHA-256
        return keyGenerator.generateKey();
    }

    private static boolean validateOtp(TimeBasedOneTimePasswordGenerator totp, SecretKey secretKey, int userInputOtp, Instant now) {
        try {
            // Only allow OTP from current window (strict 90s validity)
            int expectedOtp = totp.generateOneTimePassword(secretKey, now);

            // Log the comparison OTP
            System.out.println("Expected OTP:      " + expectedOtp);
            return expectedOtp == userInputOtp;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}