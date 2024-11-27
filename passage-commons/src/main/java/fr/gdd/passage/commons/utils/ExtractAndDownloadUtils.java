package fr.gdd.passage.commons.utils;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

/**
 * A general class that aims to ease the opening or creation of datasets to benchmark.
 */
public class ExtractAndDownloadUtils {

    private static final Logger log = LoggerFactory.getLogger(ExtractAndDownloadUtils.class);

    final static int DEFAULT_BUFFER_SIZE = 1024;

    /**
     * Download the dataset from remote URL.
     * @param path The file location to download to.
     * @param url The URL of the content to download.
     */
    static public void download(Path path, String url) {
        if (!Files.exists(path)) {
            log.info("Starting the download…");

            path.toFile().getParentFile().mkdirs(); // creating parents folder if they do not exist

            try (BufferedInputStream in = new BufferedInputStream(new URL(url).openStream());
                 FileOutputStream fileOutputStream = new FileOutputStream(path.toString())) {
                byte dataBuffer[] = new byte[DEFAULT_BUFFER_SIZE];
                int bytesRead;
                while ((bytesRead = in.read(dataBuffer, 0, dataBuffer.length)) != -1) {
                    fileOutputStream.write(dataBuffer, 0, bytesRead);
                }
                fileOutputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Extract the dataset archive and keep the whitelisted files.
     * @param archive The archive file location.
     * @param outDir The directory to extract to.
     * @param whitelist The whitelisted files to extract that serve as file output names.
     */
    static public void extract(Path archive, Path outDir, List<String> whitelist) {
        log.info("Starting the unarchiving. This process may take time…");
        try {
            extractTARBZ2(archive, outDir, whitelist);
        } catch (Exception e) {
            extractBZ2only(archive, outDir, whitelist);
        }
    }

    /**
     * Sometimes, even with extension `tar.bz2`, the archive does not contain `tar`
     * headers and metadata which will crash the execution. Instead, `bz2` only must
     * run. In such case, the outfile is the only whitelisted name.
     * @param archive The path to the archive containing compressed triples.
     * @param outDir The path to the directory where to uncompress.
     * @param whitelist The list containing one name, that of the extracted file.
     */
    static public void extractBZ2only(Path archive, Path outDir, List<String> whitelist) {
        Path entryExtractPath = Paths.get(outDir.toString(), whitelist.get(0));
        if (entryExtractPath.toFile().exists()) {
            log.info("Skipping the extraction of {}", whitelist.get(0));
            return;
        }

        try {
            FileInputStream in = new FileInputStream(archive.toString());
            InputStream bin = new BufferedInputStream(in);
            BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(bin);

            try (FileOutputStream writer = new FileOutputStream(entryExtractPath.toFile())) {
                byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
                int readCount = -1;
                while ((readCount = bzIn.read(buffer, 0, buffer.length)) > 0) {
                    writer.write(buffer, 0, readCount);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }



    /**
     * Extract the dataset `tar.bz2` archive and keep the whitelisted files.
     * @param archive The archive file location.
     * @param outDir The directory to extract to.
     * @param whitelist The whitelisted files to extract.
     */
    static public void extractTARBZ2(Path archive, Path outDir, List<String> whitelist) throws IOException {

        try {
            FileInputStream in = new FileInputStream(archive.toString());
            InputStream bin = new BufferedInputStream(in);
            BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(bin);
            TarArchiveInputStream tarIn = new TarArchiveInputStream(bzIn, true);


            if (!Files.exists(outDir)) {
                Files.createDirectory(outDir);
            }

            ArchiveEntry entry = tarIn.getNextEntry();
            while (!Objects.isNull(entry)) {
                if (entry.getSize() < 1) {
                    continue;
                }

                Path entryExtractPath = Paths.get(outDir.toString(), entry.getName());
                if (Files.exists(entryExtractPath) || !whitelist.contains(entry.getName())) {
                    log.info("Skipping file {}…", entryExtractPath);
                    // still slows… for it must read the bytes to skip them
                    tarIn.skip(entry.getSize());
                    entry = tarIn.getNextEntry();
                    continue;
                }
                log.info("Extracting file {}…", entryExtractPath);
                entryExtractPath.toFile().createNewFile();
                try (FileOutputStream writer = new FileOutputStream(entryExtractPath.toFile())) {
                    byte[] dataBuffer = new byte[1024];
                    int bytesRead;
                    while ((bytesRead = tarIn.read(dataBuffer, 0, 1024)) != -1) {
                        writer.write(dataBuffer, 0, bytesRead);
                    }
                    writer.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                entry = tarIn.getNextEntry();
            }
            tarIn.close();
        } catch (Exception e){
            // e.printStackTrace();
            throw e;
        }

    }


}
