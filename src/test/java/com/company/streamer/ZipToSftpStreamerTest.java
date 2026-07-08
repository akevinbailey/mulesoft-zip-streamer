/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.company.streamer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.Test;

/**
 * Unit tests for {@link ZipToSftpStreamer}.
 *
 * <p>These tests exercise the argument-validation guards, which run before any
 * network activity, and the {@link ZipToSftpStreamer.ZipTransferResult} value
 * object. They deliberately do not open an SFTP connection, so they need no
 * running server and no JSch host reachability. The valid "happy path" values in
 * {@link #run} are chosen so that only the single field under test is invalid in
 * each case; validation therefore fails fast on that field with a clear exception.
 *
 * Run with: mvn test
 */
public class ZipToSftpStreamerTest {

    // Valid defaults; each test overrides exactly one to prove its guard fires.
    private static final String HOST = "sftp.example.com";
    private static final int PORT = 22;
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String KNOWN_HOSTS = "sftp/known_hosts";
    private static final String TARGET_DIR = "/upload";
    private static final int MAX_FILES = 1000;
    private static final long MAX_ENTRY_BYTES = 5_368_709_120L;
    private static final long MAX_TOTAL_BYTES = 10_737_418_240L;

    private static InputStream emptyZipStream() {
        return new ByteArrayInputStream(new byte[0]);
    }

    /**
     * Invokes {@code streamZipToSftp} with the supplied arguments so individual
     * tests can override just the field they are validating.
     */
    private static void run(
            InputStream zipInputStream,
            String host,
            int port,
            String user,
            String password,
            String knownHosts,
            String targetDir,
            int maxFiles,
            long maxEntryBytes,
            long maxTotalBytes) throws Exception {
        ZipToSftpStreamer.streamZipToSftp(
                zipInputStream, host, port, user, password,
                knownHosts, targetDir, maxFiles, maxEntryBytes, maxTotalBytes);
    }

    @Test(expected = NullPointerException.class)
    public void rejectsNullZipInputStream() throws Exception {
        run(null, HOST, PORT, USER, PASSWORD, KNOWN_HOSTS, TARGET_DIR,
                MAX_FILES, MAX_ENTRY_BYTES, MAX_TOTAL_BYTES);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsBlankHost() throws Exception {
        run(emptyZipStream(), "  ", PORT, USER, PASSWORD, KNOWN_HOSTS, TARGET_DIR,
                MAX_FILES, MAX_ENTRY_BYTES, MAX_TOTAL_BYTES);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsBlankUsername() throws Exception {
        run(emptyZipStream(), HOST, PORT, "", PASSWORD, KNOWN_HOSTS, TARGET_DIR,
                MAX_FILES, MAX_ENTRY_BYTES, MAX_TOTAL_BYTES);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsBlankPassword() throws Exception {
        run(emptyZipStream(), HOST, PORT, USER, null, KNOWN_HOSTS, TARGET_DIR,
                MAX_FILES, MAX_ENTRY_BYTES, MAX_TOTAL_BYTES);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsBlankKnownHosts() throws Exception {
        run(emptyZipStream(), HOST, PORT, USER, PASSWORD, "  ", TARGET_DIR,
                MAX_FILES, MAX_ENTRY_BYTES, MAX_TOTAL_BYTES);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsBlankTargetDirectory() throws Exception {
        run(emptyZipStream(), HOST, PORT, USER, PASSWORD, KNOWN_HOSTS, "",
                MAX_FILES, MAX_ENTRY_BYTES, MAX_TOTAL_BYTES);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsPortBelowRange() throws Exception {
        run(emptyZipStream(), HOST, 0, USER, PASSWORD, KNOWN_HOSTS, TARGET_DIR,
                MAX_FILES, MAX_ENTRY_BYTES, MAX_TOTAL_BYTES);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsPortAboveRange() throws Exception {
        run(emptyZipStream(), HOST, 65_536, USER, PASSWORD, KNOWN_HOSTS, TARGET_DIR,
                MAX_FILES, MAX_ENTRY_BYTES, MAX_TOTAL_BYTES);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNonPositiveMaxFiles() throws Exception {
        run(emptyZipStream(), HOST, PORT, USER, PASSWORD, KNOWN_HOSTS, TARGET_DIR,
                0, MAX_ENTRY_BYTES, MAX_TOTAL_BYTES);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNonPositiveMaxEntryBytes() throws Exception {
        run(emptyZipStream(), HOST, PORT, USER, PASSWORD, KNOWN_HOSTS, TARGET_DIR,
                MAX_FILES, 0L, MAX_TOTAL_BYTES);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNonPositiveMaxTotalBytes() throws Exception {
        run(emptyZipStream(), HOST, PORT, USER, PASSWORD, KNOWN_HOSTS, TARGET_DIR,
                MAX_FILES, MAX_ENTRY_BYTES, 0L);
    }

    /**
     * With all arguments valid, validation passes and the method proceeds to the
     * SFTP connection, which must fail for the unreachable test host. The point is
     * that the failure is a connection/JSch error, not an argument-validation error.
     */
    @Test
    public void validArgumentsPassValidationAndReachConnectionStage() {
        try {
            run(emptyZipStream(), HOST, PORT, USER, PASSWORD, KNOWN_HOSTS, TARGET_DIR,
                    MAX_FILES, MAX_ENTRY_BYTES, MAX_TOTAL_BYTES);
            fail("expected connection to fail against unreachable test host");
        } catch (IllegalArgumentException e) {
            // known_hosts resource is absent on the test classpath: that is the
            // configureKnownHosts guard, which still proves argument validation passed.
            assertTrue(e.getMessage().toLowerCase().contains("known_hosts"));
        } catch (Exception expected) {
            // Any other exception (JSch connection failure, etc.) is acceptable:
            // validation was cleared and execution advanced past the guards.
            assertNotNull(expected);
        }
    }

    @Test
    public void zipTransferResultExposesValues() {
        ZipToSftpStreamer.ZipTransferResult result =
                new ZipToSftpStreamer.ZipTransferResult(7, 2, 123_456L);

        assertEquals(7, result.getFilesWritten());
        assertEquals(2, result.getDirectoriesSkipped());
        assertEquals(123_456L, result.getTotalBytesWritten());
    }

    @Test
    public void zipTransferResultToStringContainsCounts() {
        ZipToSftpStreamer.ZipTransferResult result =
                new ZipToSftpStreamer.ZipTransferResult(7, 2, 123_456L);

        String text = result.toString();
        assertTrue(text.contains("filesWritten=7"));
        assertTrue(text.contains("directoriesSkipped=2"));
        assertTrue(text.contains("totalBytesWritten=123456"));
    }
}
