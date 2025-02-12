package tech.turso.core;

import static tech.turso.utils.ByteArrayUtils.stringToUtf8ByteArray;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import tech.turso.LimboErrorCode;
import tech.turso.annotations.NativeInvocation;
import tech.turso.annotations.VisibleForTesting;
import tech.turso.utils.LimboExceptionUtils;
import tech.turso.utils.Logger;
import tech.turso.utils.LoggerFactory;

/** This class provides a thin JNI layer over the SQLite3 C API. */
public final class LimboDB implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(LimboDB.class);
  // Pointer to database instance
  private long dbPointer;
  private boolean isOpen;

  private final String url;
  private final String filePath;
  private static boolean isLoaded;

  static {
    if ("The Android Project".equals(System.getProperty("java.vm.vendor"))) {
      // TODO
    } else {
      // continue with non Android execution path
      isLoaded = false;
    }
  }

  /**
   * Enum representing different architectures and their corresponding library paths and file
   * extensions.
   */
  enum Architecture {
    MACOS_ARM64("libs/macos_arm64/lib_limbo_java.dylib", ".dylib"),
    MACOS_X86("libs/macos_x86/lib_limbo_java.dylib", ".dylib"),
    WINDOWS("libs/windows/lib_limbo_java.dll", ".dll"),
    UNSUPPORTED("", "");

    private final String libPath;
    private final String fileExtension;

    Architecture(String libPath, String fileExtension) {
      this.libPath = libPath;
      this.fileExtension = fileExtension;
    }

    public String getLibPath() {
      return libPath;
    }

    public String getFileExtension() {
      return fileExtension;
    }

    public static Architecture detect() {
      String osName = System.getProperty("os.name").toLowerCase();
      String osArch = System.getProperty("os.arch").toLowerCase();

      if (osName.contains("mac")) {
        if (osArch.contains("aarch64") || osArch.contains("arm64")) {
          return MACOS_ARM64;
        } else if (osArch.contains("x86_64") || osArch.contains("amd64")) {
          return MACOS_X86;
        }
      } else if (osName.contains("win")) {
        return WINDOWS;
      }

      return UNSUPPORTED;
    }
  }

  /**
   * This method attempts to load the native library required for Limbo operations. It first tries
   * to load the library from the system's library path using {@link #loadFromSystemPath()}. If that
   * fails, it attempts to load the library from the JAR file using {@link #loadFromJar()}. If
   * either method succeeds, the `isLoaded` flag is set to true. If both methods fail, an {@link
   * InternalError} is thrown indicating that the necessary native library could not be loaded.
   *
   * @throws InternalError if the native library cannot be loaded from either the system path or the
   *     JAR file.
   */
  public static void load() {
    if (isLoaded) {
      return;
    }

    if (loadFromSystemPath() || loadFromJar()) {
      isLoaded = true;
      return;
    }

    throw new InternalError("Unable to load necessary native library");
  }

  /**
   * Load the native library from the system path.
   *
   * <p>This method attempts to load the native library named "_limbo_java" from the system's
   * library path. If the library is successfully loaded, the `isLoaded` flag is set to true.
   *
   * @return true if the library was successfully loaded, false otherwise.
   */
  private static boolean loadFromSystemPath() {
    try {
      System.loadLibrary("_limbo_java");
      return true;
    } catch (Throwable t) {
      logger.info("Unable to load from default path: {}", String.valueOf(t));
    }

    return false;
  }

  /**
   * Load the native library from the JAR file.
   *
   * <p>By default, native libraries are packaged within the JAR file. This method extracts the
   * appropriate native library for the current operating system and architecture from the JAR and
   * loads it.
   *
   * @return true if the library was successfully loaded, false otherwise.
   */
  private static boolean loadFromJar() {
    Architecture arch = Architecture.detect();
    if (arch == Architecture.UNSUPPORTED) {
      logger.info("Unsupported OS or architecture");
      return false;
    }

    try {
      InputStream is = LimboDB.class.getClassLoader().getResourceAsStream(arch.getLibPath());
      assert is != null;
      File file = convertInputStreamToFile(is, arch);
      System.load(file.getPath());
      return true;
    } catch (Throwable t) {
      logger.info("Unable to load from jar: {}", String.valueOf(t));
    }

    return false;
  }

  private static File convertInputStreamToFile(InputStream is, Architecture arch)
      throws IOException {
    File tempFile = File.createTempFile("lib", arch.getFileExtension());
    tempFile.deleteOnExit();

    try (FileOutputStream os = new FileOutputStream(tempFile)) {
      int read;
      byte[] bytes = new byte[1024];

      while ((read = is.read(bytes)) != -1) {
        os.write(bytes, 0, read);
      }
    }

    return tempFile;
  }

  /**
   * @param url e.g. "jdbc:sqlite:fileName
   * @param filePath e.g. path to file
   */
  public static LimboDB create(String url, String filePath) throws SQLException {
    return new LimboDB(url, filePath);
  }

  // TODO: receive config as argument
  private LimboDB(String url, String filePath) {
    this.url = url;
    this.filePath = filePath;
  }

  // TODO: add support for JNI
  public native void interrupt();

  public boolean isClosed() {
    return !this.isOpen;
  }

  public boolean isOpen() {
    return this.isOpen;
  }

  public void open(int openFlags) throws SQLException {
    open0(filePath, openFlags);
  }

  private void open0(String filePath, int openFlags) throws SQLException {
    if (isOpen) {
      throw LimboExceptionUtils.buildLimboException(
          LimboErrorCode.LIMBO_ETC.code, "Already opened");
    }

    byte[] filePathBytes = stringToUtf8ByteArray(filePath);
    if (filePathBytes == null) {
      throw LimboExceptionUtils.buildLimboException(
          LimboErrorCode.LIMBO_ETC.code,
          "File path cannot be converted to byteArray. File name: " + filePath);
    }

    dbPointer = openUtf8(filePathBytes, openFlags);
    isOpen = true;
  }

  private native long openUtf8(byte[] file, int openFlags) throws SQLException;

  public long connect() throws SQLException {
    return connect0(dbPointer);
  }

  private native long connect0(long databasePtr) throws SQLException;

  @Override
  public void close() throws Exception {
    if (!isOpen) return;

    close0(dbPointer);
    isOpen = false;
  }

  private native void close0(long databasePtr) throws SQLException;

  @VisibleForTesting
  native void throwJavaException(int errorCode) throws SQLException;

  /**
   * Throws formatted SQLException with error code and message.
   *
   * @param errorCode Error code.
   * @param errorMessageBytes Error message.
   */
  @NativeInvocation(invokedFrom = "limbo_db.rs")
  private void throwLimboException(int errorCode, byte[] errorMessageBytes) throws SQLException {
    LimboExceptionUtils.throwLimboException(errorCode, errorMessageBytes);
  }
}
