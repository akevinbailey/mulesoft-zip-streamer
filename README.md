# MuleSoft Streaming Large ZIP File Entries from AWS S3 to SFTP — Custom Java Class

> **⚠️ This project is provided for example purposes only.** It is a reference
> implementation intended to illustrate an approach — not a supported, production-ready
> library. Review, adapt, and test it against your own requirements before any real use.
> It is provided "as is", without warranty of any kind (see [License](#license)).

## Scenario

The application receives a large ZIP file from AWS S3 Bucket storage, typically around **2 GB**, and must send each individual file inside the ZIP archive, up to approximately **1000 files**, to an SFTP location.

A simple Mule flow can use the MuleSoft S3 Connector to retrieve the ZIP file, then use `For Each` to iterate over extracted files and write them to SFTP. However, on MuleSoft CloudHub 2 this design can fail with local disk or temporary-file exhaustion because large streams may be buffered to the worker's ephemeral local storage.

The goal is to process the ZIP with minimal memory and minimal local temporary storage.

## Recommended Answer

Do **not** use DataWeave to split a large binary ZIP archive.

The recommended design is:

```text
S3 Get Object as non-repeatable stream
        |
        v
Java ZipInputStream
        |
        v
For each ZIP entry reached sequentially
        |
        v
Write that entry directly to SFTP output stream
```

This uses a fixed-size byte buffer, for example 64 KB or 1 MB, and does **not** materialize either the 2 GB ZIP file or the expanded files on the Mule CloudHub 2 worker's local disk.

## Why the Current Flow Runs Out of Space

Mule 4 uses repeatable streams by default. With the file-store repeatable stream strategy, Mule starts with an in-memory buffer and then writes larger streams to temporary disk. That default is helpful when multiple processors need to re-read the payload, but it is dangerous for large binary files on CloudHub 2.

For this workload, the out-of-space error is probably not caused by S3 itself. It is more likely caused by one or more of the following:

- Mule repeatable stream buffering.
- The ZIP archive being materialized locally.
- Extracted ZIP entries being held in memory or temporary disk.
- A `For Each` design that forces a collection-like representation of extracted entries.
- Logging, transformation, or variable assignment that reads the stream before it reaches the final writer.

CloudHub 2 local storage is ephemeral and limited by replica size, so a 2 GB compressed ZIP can easily exceed safe local working space once buffers, expanded entries, retries, and temporary files are included.

## DataWeave Assessment

DataWeave is not the right tool for this specific problem.

DataWeave streaming is appropriate for streamable formats such as CSV records, JSON array items, and XML collections. A ZIP file is a binary archive format. DataWeave does not provide a native model that says: "emit one Mule event for the next ZIP entry as soon as the ZIP parser reaches that entry."

The MuleSoft Compression Module can extract ZIP archives, but for this workload it is not the safest CloudHub 2 design. The extract operation returns an object representation of archive entries, and examples commonly iterate over that object. For a large 2 GB ZIP with up to 1000 files, this can create too much pressure on heap and local temporary storage unless load testing proves otherwise.

## Best Mule-Only Option

If you can change the producer contract, the best Mule-components-only architecture is to avoid the large ZIP entirely:

1. Store each individual file directly in S3.
2. Store all files under a batch prefix, for example:

   ```text
   s3://source-bucket/batches/2026-07-08-001/file001.dat
   s3://source-bucket/batches/2026-07-08-001/file002.dat
   s3://source-bucket/batches/2026-07-08-001/file003.dat
   ```

3. Optionally store a manifest file.
4. Mule lists the S3 objects under the batch prefix.
5. Mule retrieves each S3 object as a `non-repeatable-stream`.
6. Mule writes the payload directly to SFTP.

That design uses normal MuleSoft components and connectors and avoids ZIP extraction completely.

## Recommended Mule + Java Design

If the input must remain one large ZIP, use Mule for orchestration and Java for streaming ZIP extraction.

### Flow Pattern

```text
Scheduler / HTTP / Event Trigger
        |
        v
S3 Get Object
        |  non-repeatable-stream
        v
Java Invoke Static
        |  payload is InputStream
        v
ZipToSftpStreamer.streamZipToSftp(...)
        |
        v
Log result summary only
```

### S3 Get Object Example

The critical detail is to use `non-repeatable-stream`.

```xml
<s3:get-object
    config-ref="Amazon_S3_Config"
    bucketName="${s3.bucket}"
    objectKey="#[vars.objectKey]"
    outputMimeType="application/octet-stream">
    <non-repeatable-stream/>
</s3:get-object>
```

### Java Invoke Example

```xml
<java:invoke-static
    class="com.company.streamer.ZipToSftpStreamer"
    method="streamZipToSftp(java.io.InputStream, java.lang.String, int, java.lang.String, java.lang.String, java.lang.String, java.lang.String, int, long, long)">
    <java:args><![CDATA[
        #[{
            arg0: payload,
            arg1: p('sftp.host'),
            arg2: p('sftp.port') as Number,
            arg3: p('sftp.username'),
            arg4: p('secure::sftp.password'),
            arg5: p('sftp.knownHostsPathOrResource'),
            arg6: p('sftp.targetDirectory'),
            arg7: 1000,
            arg8: 5368709120,
            arg9: 10737418240
        }]
    ]]></java:args>
</java:invoke-static>
```

Example limits above:

- `arg7`: maximum files inside the ZIP: `1000`.
- `arg8`: maximum uncompressed size for one ZIP entry: `5368709120` bytes, or 5 GiB.
- `arg9`: maximum total uncompressed size across the ZIP: `10737418240` bytes, or 10 GiB.

Adjust these limits for your business case.

## Java Class

The companion file is:

```text
src/main/java/com/company/streamer/ZipToSftpStreamer.java
```

It uses package:

```java
package com.company.streamer;
```

The class does the following:

- Accepts the Mule payload as a Java `InputStream`.
- Wraps the stream in `ZipInputStream`.
- Calls `getNextEntry()` to advance sequentially through the ZIP archive.
- Streams the current entry directly into an SFTP remote output stream.
- Uses a fixed 1 MB transfer buffer.
- Creates remote directories if needed.
- Writes to a temporary `.part-*` file first.
- Renames the temporary file to the final file name after the entry is fully written.
- Deletes the partial remote file on failure when possible.
- Protects against ZIP Slip path traversal.
- Enforces maximum file count, maximum per-entry uncompressed size, and maximum total uncompressed size.

## Required Java Dependency

The Java class uses the JSch API package:

```java
com.jcraft.jsch
```

Use your organization's approved SFTP library artifact and version. One common modern choice is the maintained JSch fork that preserves the same Java package names:

```xml
<dependency>
    <groupId>com.github.mwiede</groupId>
    <artifactId>jsch</artifactId>
    <version>${jsch.version}</version>
</dependency>
```

Pin `${jsch.version}` to a specific version approved by your security and dependency management process.

## Known Hosts Handling

The Java class requires a known hosts file or classpath resource. This is intentional.

For production, do not disable SFTP host key checking. Package a `known_hosts` file with the Mule application or mount/provide it through your approved CloudHub 2 deployment mechanism.

Example property:

```properties
sftp.knownHostsPathOrResource=sftp/known_hosts
```

Example resource layout:

```text
src/main/resources/sftp/known_hosts
```

The Java code first checks whether the value is a filesystem path. If not, it tries to load it as a classpath resource.

## Critical MuleSoft Rules

Use `non-repeatable-stream` when retrieving the ZIP from S3.

Do **not** do the following before the Java invocation:

- Do not log the full payload.
- Do not convert the payload to a String.
- Do not store the payload in a variable if that causes it to be consumed or repeatably buffered.
- Do not run the payload through DataWeave.
- Do not use a processor that reads the stream before Java receives it.
- Do not split the ZIP into a list of entry streams and then use Mule `For Each`.

With a non-repeatable stream, the payload can be consumed only once. The Java method should be the first component that reads the S3 object payload.

## Why Not Return ZIP Entries Back to Mule?

Avoid this pattern:

```text
Java ZIP parser -> returns List<InputStream> -> Mule For Each -> SFTP Write
```

That is usually wrong for ZIP streaming.

`ZipInputStream` is sequential. Once the code advances to the next ZIP entry, the previous entry's stream is no longer independently available. Returning a list of entry streams generally forces one of these bad outcomes:

- Buffer each entry in memory.
- Buffer each entry to local temporary files.
- Return broken streams that cannot be read later.
- Accidentally keep the whole ZIP or many expanded files in memory/disk.

The Java method should complete the SFTP write while it is positioned on the current ZIP entry.

## Security Controls

The Java implementation includes several safety controls that are important for enterprise file ingestion.

### ZIP Slip Protection

ZIP archives can contain dangerous entry names such as:

```text
../../somewhere/evil.txt
/absolute/path/file.txt
C:\\Windows\\system32\\file.txt
```

The code normalizes and rejects unsafe paths before writing to SFTP.

### ZIP Bomb Protection

A small compressed ZIP can expand into a very large uncompressed payload. The code protects against this using:

- Maximum number of files.
- Maximum uncompressed size per entry.
- Maximum total uncompressed size across the archive.

### Partial File Protection

Each remote file is first written as:

```text
final-name.part-<uuid>
```

After the entry is fully written, the code renames it to the final target name. If the app fails while writing, the final filename is not left as a corrupt partial file.

## Operational Notes

### Restart and Resume

The Java streaming approach minimizes memory and disk usage, but it is not automatically resumable at the middle of a ZIP file.

If the application fails halfway through a 2 GB ZIP, it normally needs to restart the ZIP stream from S3 and process again. The `.part-*` naming strategy helps avoid corrupt final files, but it does not provide full checkpoint/restart semantics.

If restart/resume is a hard requirement, consider an external worker architecture or add an idempotency manifest.

### Idempotency

Recommended idempotency options:

- Include a manifest file listing expected entries and checksums.
- Write a completion marker after each file is successfully transferred.
- Use a database table to track batch ID, entry name, size, checksum, and transfer status.
- Skip files that were already successfully transferred.
- Avoid blindly overwriting final remote files unless that is the intended business behavior.

### Logging

Log metadata only:

- S3 bucket.
- S3 key.
- ZIP entry name.
- Entry byte count.
- Number of files written.
- Total bytes written.
- Duration.

Do not log the payload or file contents.

## Recommended Final Position

For CloudHub 2 and 2 GB ZIP files, the recommended implementation is:

```text
S3 Get Object with non-repeatable-stream
        -> Java ZipInputStream
        -> direct SFTP output stream per ZIP entry
```

Avoid DataWeave and avoid Mule Compression Extract for this workload unless load testing proves that the implementation remains fully streamed with acceptable temporary storage use.

If the file transfer is long-running, business-critical, or requires restart/resume, consider moving the unzip-to-SFTP workload to AWS ECS/Fargate or AWS Batch and let MuleSoft orchestrate the process.

## References

- MuleSoft streaming overview: https://docs.mulesoft.com/mule-runtime/latest/streaming-about
- MuleSoft DataWeave streaming: https://docs.mulesoft.com/dataweave/latest/dataweave-streaming
- MuleSoft Amazon S3 Connector reference: https://docs.mulesoft.com/amazon-s3-connector/latest/amazon-s3-connector-reference
- MuleSoft SFTP Connector reference: https://docs.mulesoft.com/sftp-connector/latest/sftp-documentation
- MuleSoft Compression Module reference: https://docs.mulesoft.com/compression-module/latest/compression-documentation
- MuleSoft Compression Module archive extract example: https://docs.mulesoft.com/compression-module/latest/compression-module-archive-extract-example
- MuleSoft Java Module reference: https://docs.mulesoft.com/java-module/latest/java-reference
- Java ZipInputStream API: https://docs.oracle.com/javase/8/docs/api/java/util/zip/ZipInputStream.html

## License

Licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for the
full text. You may obtain a copy of the License at
<http://www.apache.org/licenses/LICENSE-2.0>.

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied.
