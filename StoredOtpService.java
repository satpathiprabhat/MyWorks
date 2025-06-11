import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;

public class TotpFixedWindow {

    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final int OTP_DIGITS = 6;
    private static final int TIME_STEP_SECONDS = 90;

    // Step 1: Generate OTP and return time window counter
    public static OtpWithCounter generateOtp(byte[] secret) throws NoSuchAlgorithmException, InvalidKeyException {
        long currentEpoch = Instant.now().getEpochSecond();
        long counter = currentEpoch / TIME_STEP_SECONDS;
        int otp = generateFromCounter(secret, counter);

        System.out.println("Generated OTP: " + otp);
        System.out.println("Generated at time (sec): " + currentEpoch + " | Counter: " + counter);
        return new OtpWithCounter(otp, counter);
    }

    // Step 2: Validate OTP against the exact counter used during generation
    public static boolean validateOtp(byte[] secret, int otpToValidate, long generationCounter) throws Exception {
        int expectedOtp = generateFromCounter(secret, generationCounter);
        System.out.println("Expected OTP for counter " + generationCounter + ": " + expectedOtp);
        return expectedOtp == otpToValidate;
    }

    // Core logic: Generate OTP from counter
    private static int generateFromCounter(byte[] secret, long counter) throws NoSuchAlgorithmException, InvalidKeyException {
        byte[] counterBytes = ByteBuffer.allocate(8).putLong(counter).array();

        Mac hmac = Mac.getInstance(HMAC_ALGORITHM);
        SecretKeySpec keySpec = new SecretKeySpec(secret, HMAC_ALGORITHM);
        hmac.init(keySpec);
        byte[] hash = hmac.doFinal(counterBytes);

        int offset = hash[hash.length - 1] & 0x0F;
        int binary =
            ((hash[offset] & 0x7F) << 24) |
            ((hash[offset + 1] & 0xFF) << 16) |
            ((hash[offset + 2] & 0xFF) << 8) |
            (hash[offset + 3] & 0xFF);

        return binary % (int) Math.pow(10, OTP_DIGITS);
    }

    // Decode base64 key for demo
    public static byte[] decodeBase64Key(String base64) {
        return Base64.getDecoder().decode(base64);
    }

    // Structure to return OTP and time window
    public static class OtpWithCounter {
        public final int otp;
        public final long counter;
        public OtpWithCounter(int otp, long counter) {
            this.otp = otp;
            this.counter = counter;
        }
    }

    public static void main(String[] args) throws Exception {
        // Example secret
        String base64Key = "2C6Z9vW2sfzjbl2xDkGgEQ==";
        byte[] secret = decodeBase64Key(base64Key);

        // OTP Generation
        OtpWithCounter result = generateOtp(secret);

        // Simulate validation with exact counter
        boolean isValid = validateOtp(secret, result.otp, result.counter);
        System.out.println("Is OTP valid? " + isValid);
    }
}