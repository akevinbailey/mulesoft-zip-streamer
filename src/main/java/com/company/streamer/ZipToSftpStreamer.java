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

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Streams a ZIP archive from an InputStream and writes each ZIP entry directly to SFTP.
 *
 * Intended MuleSoft usage:
 *   S3 Get Object with non-repeatable-stream -> Java invoke-static -> this class.
 *
 * This implementation avoids materializing either the ZIP file or extracted entries
 * on the Mule worker's local disk. It uses a fixed-size transfer buffer.
 */
public final class ZipToSftpStreamer {

    private static final int BUFFER_SIZE = 1024 * 1024; // 1 MB
    private static final int CONNECT_TIMEOUT_MILLIS = 30_000;

    private ZipToSftpStreamer() {
        // Utility class.
    }

    /**
     * Streams a ZIP file to SFTP one entry at a time.
     *
     * @param zipInputStream the ZIP file stream, normally Mule payload from S3 Get Object
     * @param sftpHost SFTP host name
     * @param sftpPort SFTP port, normally 22
     * @param sftpUsername SFTP username
     * @param sftpPassword SFTP password. For production, key-based auth is usually preferred.
     * @param knownHostsPathOrResource local file path or classpath resource containing known_hosts entries
     * @param targetDirectory target remote SFTP directory
     * @param maxFiles maximum number of files allowed inside the ZIP
     * @param maxEntryBytes maximum uncompressed bytes allowed for a single ZIP entry
     * @param maxTotalUncompressedBytes maximum total uncompressed bytes allowed across all entries
     * @return transfer result summary
     * @throws Exception if streaming, ZIP validation, or SFTP transfer fails
     */
    public static ZipTransferResult streamZipToSftp(
            InputStream zipInputStream,
            String sftpHost,
            int sftpPort,
            String sftpUsername,
            String sftpPassword,
            String knownHostsPathOrResource,
            String targetDirectory,
            int maxFiles,
            long maxEntryBytes,
            long maxTotalUncompressedBytes
    ) throws Exception {

        Objects.requireNonNull(zipInputStream, "zipInputStream must not be null");
        requireNotBlank(sftpHost, "sftpHost");
        requireNotBlank(sftpUsername, "sftpUsername");
        requireNotBlank(sftpPassword, "sftpPassword");
        requireNotBlank(knownHostsPathOrResource, "knownHostsPathOrResource");
        requireNotBlank(targetDirectory, "targetDirectory");

        if (sftpPort <= 0 || sftpPort > 65535) {
            throw new IllegalArgumentException("sftpPort must be between 1 and 65535");
        }
        if (maxFiles <= 0) {
            throw new IllegalArgumentException("maxFiles must be greater than zero");
        }
        if (maxEntryBytes <= 0) {
            throw new IllegalArgumentException("maxEntryBytes must be greater than zero");
        }
        if (maxTotalUncompressedBytes <= 0) {
            throw new IllegalArgumentException("maxTotalUncompressedBytes must be greater than zero");
        }

        JSch jsch = new JSch();
        configureKnownHosts(jsch, knownHostsPathOrResource);

        Session session = jsch.getSession(sftpUsername, sftpHost, sftpPort);
        session.setPassword(sftpPassword);

        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "yes");
        session.setConfig(config);

        ChannelSftp sftp = null;

        int filesWritten = 0;
        int directoriesSkipped = 0;
        long totalBytesWritten = 0L;

        try {
            session.connect(CONNECT_TIMEOUT_MILLIS);

            sftp = (ChannelSftp) session.openChannel("sftp");
            sftp.connect(CONNECT_TIMEOUT_MILLIS);

            try (ZipInputStream zip = new ZipInputStream(
                    new BufferedInputStream(zipInputStream, BUFFER_SIZE))) {

                byte[] buffer = new byte[BUFFER_SIZE];
                ZipEntry entry;

                while ((entry = zip.getNextEntry()) != null) {
                    String entryName = entry.getName();

                    if (entry.isDirectory()) {
                        directoriesSkipped++;
                        zip.closeEntry();
                        continue;
                    }

                    filesWritten++;
                    if (filesWritten > maxFiles) {
                        throw new SecurityException("ZIP contains more files than allowed: " + maxFiles);
                    }

                    String safeRelativePath = safeZipEntryPath(entryName);
                    String remotePath = joinRemotePath(targetDirectory, safeRelativePath);
                    String remoteTempPath = remotePath + ".part-" + UUID.randomUUID();

                    ensureDirectoryExists(sftp, parentDirectory(remotePath));

                    long entryBytesWritten = 0L;
                    boolean completed = false;

                    try (OutputStream out = sftp.put(remoteTempPath, ChannelSftp.OVERWRITE)) {
                        int read;
                        while ((read = zip.read(buffer)) != -1) {
                            entryBytesWritten += read;
                            totalBytesWritten += read;

                            if (entryBytesWritten > maxEntryBytes) {
                                throw new SecurityException(
                                        "ZIP entry exceeds max allowed size: " + entryName);
                            }

                            if (totalBytesWritten > maxTotalUncompressedBytes) {
                                throw new SecurityException(
                                        "ZIP total uncompressed size exceeds max allowed size");
                            }

                            out.write(buffer, 0, read);
                        }

                        completed = true;
                    } finally {
                        zip.closeEntry();

                        if (!completed) {
                            removeQuietly(sftp, remoteTempPath);
                        }
                    }

                    // Replace existing target where supported by the SFTP server.
                    removeQuietly(sftp, remotePath);
                    sftp.rename(remoteTempPath, remotePath);
                }
            }

            return new ZipTransferResult(filesWritten, directoriesSkipped, totalBytesWritten);

        } finally {
            if (sftp != null && sftp.isConnected()) {
                sftp.disconnect();
            }

            if (session.isConnected()) {
                session.disconnect();
            }
        }
    }

    private static void configureKnownHosts(JSch jsch, String knownHostsPathOrResource)
            throws JSchException, IOException {

        File knownHostsFile = new File(knownHostsPathOrResource);
        if (knownHostsFile.isFile()) {
            jsch.setKnownHosts(knownHostsFile.getAbsolutePath());
            return;
        }

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream knownHostsStream = classLoader == null
                ? ZipToSftpStreamer.class.getClassLoader().getResourceAsStream(knownHostsPathOrResource)
                : classLoader.getResourceAsStream(knownHostsPathOrResource);

        if (knownHostsStream == null) {
            throw new IllegalArgumentException(
                    "known_hosts file not found as file path or classpath resource: "
                            + knownHostsPathOrResource);
        }

        try (InputStream in = knownHostsStream) {
            jsch.setKnownHosts(in);
        }
    }

    private static String safeZipEntryPath(String entryName) {
        requireNotBlank(entryName, "zip entry name");

        if (entryName.indexOf('\0') >= 0) {
            throw new SecurityException("Unsafe ZIP entry path contains NUL byte");
        }

        String normalizedName = entryName.replace('\\', '/');

        if (normalizedName.startsWith("/") || normalizedName.contains(":")) {
            throw new SecurityException("Unsafe ZIP entry path: " + entryName);
        }

        Path normalizedPath = Paths.get(normalizedName).normalize();
        String normalizedPathText = normalizedPath.toString().replace('\\', '/');

        if (normalizedPathText.isEmpty()
                || normalizedPath.isAbsolute()
                || normalizedPathText.equals("..")
                || normalizedPathText.startsWith("../")) {
            throw new SecurityException("Unsafe ZIP entry path: " + entryName);
        }

        return normalizedPathText;
    }

    private static String joinRemotePath(String baseDirectory, String relativePath) {
        String base = baseDirectory.endsWith("/")
                ? baseDirectory.substring(0, baseDirectory.length() - 1)
                : baseDirectory;

        return base + "/" + relativePath;
    }

    private static String parentDirectory(String remotePath) {
        int lastSlash = remotePath.lastIndexOf('/');
        if (lastSlash <= 0) {
            return "/";
        }
        return remotePath.substring(0, lastSlash);
    }

    private static void ensureDirectoryExists(ChannelSftp sftp, String directory) throws SftpException {
        if (isBlank(directory) || "/".equals(directory)) {
            return;
        }

        String[] parts = directory.split("/");
        String current = directory.startsWith("/") ? "/" : "";

        for (String part : parts) {
            if (isBlank(part)) {
                continue;
            }

            current = "/".equals(current) ? current + part : current + "/" + part;

            try {
                SftpATTRS attrs = sftp.stat(current);
                if (!attrs.isDir()) {
                    throw new SftpException(ChannelSftp.SSH_FX_FAILURE,
                            "Remote path exists but is not a directory: " + current);
                }
            } catch (SftpException notFoundOrFailure) {
                if (notFoundOrFailure.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                    sftp.mkdir(current);
                } else {
                    throw notFoundOrFailure;
                }
            }
        }
    }

    private static void removeQuietly(ChannelSftp sftp, String remotePath) {
        try {
            sftp.rm(remotePath);
        } catch (Exception ignored) {
            // Best-effort cleanup or replacement.
        }
    }

    private static void requireNotBlank(String value, String fieldName) {
        if (isBlank(value)) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    /**
     * Result returned to Mule after the ZIP has been streamed to SFTP.
     */
    public static final class ZipTransferResult {
        private final int filesWritten;
        private final int directoriesSkipped;
        private final long totalBytesWritten;

        public ZipTransferResult(int filesWritten, int directoriesSkipped, long totalBytesWritten) {
            this.filesWritten = filesWritten;
            this.directoriesSkipped = directoriesSkipped;
            this.totalBytesWritten = totalBytesWritten;
        }

        public int getFilesWritten() {
            return filesWritten;
        }

        public int getDirectoriesSkipped() {
            return directoriesSkipped;
        }

        public long getTotalBytesWritten() {
            return totalBytesWritten;
        }

        @Override
        public String toString() {
            return "ZipTransferResult{" +
                    "filesWritten=" + filesWritten +
                    ", directoriesSkipped=" + directoriesSkipped +
                    ", totalBytesWritten=" + totalBytesWritten +
                    '}';
        }
    }
}
