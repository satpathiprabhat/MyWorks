import org.w3c.dom.*;
import javax.xml.crypto.dsig.*;
import javax.xml.crypto.dsig.dom.DOMValidateContext;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;

public class XMLSignatureValidator {

    public static void main(String[] args) throws Exception {
        // Load XML content from file
        byte[] xmlBytes = Files.readAllBytes(Paths.get("signed-request.xml")); // Path to your file
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);  // This is critical
        Document doc = dbf.newDocumentBuilder().parse(new ByteArrayInputStream(xmlBytes));

        // Find the Signature element
        NodeList sigElements = doc.getElementsByTagNameNS(XMLSignature.XMLNS, "Signature");
        if (sigElements.getLength() == 0) {
            throw new RuntimeException("No Signature element found!");
        }

        Element signatureElement = (Element) sigElements.item(0);

        // Extract Modulus and Exponent from KeyInfo
        NodeList modulusNodes = doc.getElementsByTagName("Modulus");
        NodeList exponentNodes = doc.getElementsByTagName("Exponent");

        if (modulusNodes.getLength() == 0 || exponentNodes.getLength() == 0) {
            throw new RuntimeException("Modulus or Exponent not found!");
        }

        String modulusBase64 = modulusNodes.item(0).getTextContent().replaceAll("\\s+", "");
        String exponentBase64 = exponentNodes.item(0).getTextContent().replaceAll("\\s+", "");

        byte[] modulusBytes = Base64.getDecoder().decode(modulusBase64);
        byte[] exponentBytes = Base64.getDecoder().decode(exponentBase64);

        BigInteger modulus = new BigInteger(1, modulusBytes);
        BigInteger exponent = new BigInteger(1, exponentBytes);

        RSAPublicKeySpec keySpec = new RSAPublicKeySpec(modulus, exponent);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PublicKey publicKey = keyFactory.generatePublic(keySpec);

        // Validate the signature
        XMLSignatureFactory factory = XMLSignatureFactory.getInstance("DOM");
        DOMValidateContext valContext = new DOMValidateContext(publicKey, signatureElement);
        XMLSignature signature = factory.unmarshalXMLSignature(valContext);

        boolean isValid = signature.validate(valContext);
        System.out.println("Signature valid: " + isValid);
    }
}