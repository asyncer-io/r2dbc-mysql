/*
 * Copyright 2023 asyncer.io projects
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.asyncer.r2dbc.mysql.authentication;

import io.asyncer.r2dbc.mysql.ServerVersion;
import io.asyncer.r2dbc.mysql.collation.CharCollation;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import org.jetbrains.annotations.Nullable;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

/**
 * An abstraction of the MySQL authorization plugin provider for connection phase. More information for MySQL
 * authentication type:
 * <p>
 * Execute {@code SELECT * FROM `information_schema`.`PLUGINS` WHERE `plugin_type` = 'AUTHENTICATION'} to
 * obtain more information about the authentication plugins supported by a MySQL server.
 */
public interface MySqlAuthProvider {

    /**
     * The new authentication plugin type under MySQL 8.0+. It is also the default type of MySQL 8.0.x.
     */
    String CACHING_SHA2_PASSWORD = "caching_sha2_password";

    /**
     * The most generic authentication type in MySQL 5.x.
     */
    String MYSQL_NATIVE_PASSWORD = "mysql_native_password";

    /**
     * The new authentication plugin type under MySQL 8.0+.
     */
    String SHA256_PASSWORD = "sha256_password";

    /**
     * The Old Password Authentication, it is also the only type of authentication in handshake V9.
     * <p>
     * WARNING: The hashing algorithm has broken that is used for the Old Password Authentication (as shown in
     * CVE-2000-0981).
     */
    String MYSQL_OLD_PASSWORD = "mysql_old_password";

    /**
     * The Cleartext Authentication, it is used by LDAP, PAM, AWS RDS Proxy, etc.
     */
    String MYSQL_CLEAR_PASSWORD = "mysql_clear_password";

    /**
     * Try use empty string to represent has no authentication provider when {@code Capability.PLUGIN_AUTH}
     * does not set.
     */
    String NO_AUTH_PROVIDER = "";

    /**
     * Get the built-in authentication plugin provider through the specified {@code type}.
     *
     * @param type the type name of an authentication plugin provider
     * @return the authentication plugin provider
     * @throws R2dbcPermissionDeniedException the {@code type} name not found
     */
    static MySqlAuthProvider build(String type) {
        requireNonNull(type, "type must not be null");

        switch (type) {
            case CACHING_SHA2_PASSWORD:
                return CachingSha2FastAuthProvider.getInstance();
            case MYSQL_NATIVE_PASSWORD:
                return MySqlNativeAuthProvider.getInstance();
            case MYSQL_CLEAR_PASSWORD:
                return MySqlClearAuthProvider.getInstance();
            case SHA256_PASSWORD:
                return Sha256AuthProvider.getInstance();
            case MYSQL_OLD_PASSWORD:
                return OldAuthProvider.getInstance();
            case NO_AUTH_PROVIDER:
                return NoAuthProvider.getInstance();
        }

        throw new R2dbcPermissionDeniedException("Authentication plugin '" + type + "' not found");
    }

    /**
     * The type name of the authentication plugin provider.
     *
     * @return type name
     */
    String getType();

    /**
     * Check if the authentication type should be used on SSL.
     *
     * @return {@code true} if SSL necessary
     */
    boolean isSslNecessary();

    /**
     * Generate an authorization of the current provider.
     *
     * @param password  user password
     * @param salt      password salt for hash algorithm
     * @param collation password character collation
     * @return fast authentication phase must not be null.
     */
    byte[] authentication(@Nullable CharSequence password, byte[] salt, CharCollation collation);

    /**
     * Get the next authentication plugin provider for same authentication type, or {@code this} if has not
     * next provider.
     *
     * @return the next provider
     */
    MySqlAuthProvider next();

    /**
     * Encrypts data with the RSA Public Key of MySQL server
     * @param bytesToEncrypt the data to encrypt
     * @param serverRSAPublicKeyFile the file path on the local system of the database server's RSA Public Key
     * @param serverVersion the version of the MySQL server
     * @param seed the seed bytes for rotating XOR obfuscation
     * @return the encrypted bytes
     */
    static byte[] rsaEncryption(byte[] bytesToEncrypt, String serverRsaPublicKeyFile, ServerVersion serverVersion,
    byte[] seed) {
        try {
            bytesToEncrypt = AuthUtils.rotatingXor(bytesToEncrypt, seed);

            String key = new String(Files.readAllBytes(Paths.get(serverRsaPublicKeyFile)), Charset.defaultCharset());

            int startIndex = key.indexOf("-----BEGIN PUBLIC KEY-----") + 26;
            int endIndex = key.indexOf("-----END PUBLIC KEY-----");
            key = key.substring(startIndex, endIndex);
            String publicKeyPEM = key.replaceAll(System.lineSeparator(), "");

            byte[] encoded = Base64.getDecoder().decode(publicKeyPEM);

            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            X509EncodedKeySpec keySpec = new X509EncodedKeySpec(encoded);
            RSAPublicKey pk = (RSAPublicKey) keyFactory.generatePublic(keySpec);

            Cipher cipher;
            if (serverVersion.isGreaterThanOrEqualTo(ServerVersion.create(8, 0, 5))) {
                cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
            } else {
                cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
            }
            cipher.init(Cipher.ENCRYPT_MODE, pk);
            return cipher.doFinal(bytesToEncrypt);
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getLocalizedMessage(), e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(e.getLocalizedMessage(), e);
        } catch (InvalidKeySpecException e) {
            throw new IllegalArgumentException(e.getLocalizedMessage(), e);
        } catch (NoSuchPaddingException e) {
            throw new IllegalArgumentException(e.getLocalizedMessage(), e);
        } catch (InvalidKeyException e) {
            throw new IllegalArgumentException(e.getLocalizedMessage(), e);
        } catch (IllegalBlockSizeException e) {
            throw new IllegalArgumentException(e.getLocalizedMessage(), e);
        } catch (BadPaddingException e) {
            throw new IllegalArgumentException(e.getLocalizedMessage(), e);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(e.getLocalizedMessage(), e);
        }
    }
}
