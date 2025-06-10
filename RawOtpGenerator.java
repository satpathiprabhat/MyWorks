import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;

public class RawTotpGenerator {

    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final int OTP_DIGITS = 6;
    private static final int TIME_STEP_SECONDS = 90;

    // Generate TOTP
    public static int generateOtp(byte[] secret, Instant timestamp) throws NoSuchAlgorithmException, InvalidKeyException {
        long counter = timestamp.getEpochSecond() / TIME_STEP_SECONDS;
        byte[] counterBytes = ByteBuffer.allocate(8).putLong(counter).array();

        // Step 1: HMAC(secret, counter)
        Mac hmac = Mac.getInstance(HMAC_ALGORITHM);
        SecretKeySpec keySpec = new SecretKeySpec(secret, HMAC_ALGORITHM);
        hmac.init(keySpec);
        byte[] hash = hmac.doFinal(counterBytes);

        // Step 2: Dynamic Truncation
        int offset = hash[hash.length - 1] & 0x0F;
        int binary =
            ((hash[offset] & 0x7F) << 24) |
            ((hash[offset + 1] & 0xFF) << 16) |
            ((hash[offset + 2] & 0xFF) << 8) |
            (hash[offset + 3] & 0xFF);

        // Step 3: Modulo
        int otp = binary % (int) Math.pow(10, OTP_DIGITS);

        // Debug
        System.out.println("Timestamp: " + timestamp.getEpochSecond());
        System.out.println("Counter: " + counter);
        System.out.println("OTP: " + otp);
        return otp;
    }

    // Validate OTP with optional time window tolerance (0 = strict)
    public static boolean validateOtp(byte[] secret, int otpToValidate, int allowedDriftWindows) throws Exception {
        long currentCounter = Instant.now().getEpochSecond() / TIME_STEP_SECONDS;

        for (int i = -allowedDriftWindows; i <= allowedDriftWindows; i++) {
            Instant timeToTest = Instant.ofEpochSecond((currentCounter + i) * TIME_STEP_SECONDS);
            int expectedOtp = generateOtp(secret, timeToTest);
            if (expectedOtp == otpToValidate) return true;
        }
        return false;
    }

    // Simple base64 key generator for testing
    public static byte[] decodeBase64Key(String base64) {
        return Base64.getDecoder().decode(base64);
    }

    public static void main(String[] args) throws Exception {
        // Example key (replace with your securely stored secret)
        String base64Key = "2C6Z9vW2sfzjbl2xDkGgEQ=="; // Random base64 key
        byte[] secret = decodeBase64Key(base64Key);

        // Generate OTP
        int otp = generateOtp(secret, Instant.now());

        // Validate OTP
        boolean isValid = validateOtp(secret, otp, 0); // 0 = strict validation
        System.out.println("Valid OTP? " + isValid);
    }
}