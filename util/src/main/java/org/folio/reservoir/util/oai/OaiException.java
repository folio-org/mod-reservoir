package org.folio.reservoir.util.oai;

public class OaiException extends RuntimeException {

  private final String errorCode;

  public String getErrorCode() {
    return errorCode;
  }

  public static OaiException badArgument(String msg) {
    return new OaiException("badArgument", msg);
  }

  public static OaiException badVerb(String msg) {
    return new OaiException("badVerb", msg);
  }

  public static OaiException cannotDisseminateFormat(String msg) {
    return new OaiException("cannotDisseminateFormat", msg);
  }

  public static OaiException idDoesNotExist(String msg) {
    return new OaiException("idDoesNotExist", msg);
  }

  public OaiException(String msg) {
    super(msg);
    this.errorCode = "client";
  }

  public OaiException(String errorCode, String msg) {
    super(msg);
    this.errorCode = errorCode;
  }

}
