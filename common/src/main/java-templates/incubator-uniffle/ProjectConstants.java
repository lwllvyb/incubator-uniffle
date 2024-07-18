package org.apache.uniffle.common;
public final class ProjectConstants {
  /* Project version, specified in maven property. **/
  public static final String VERSION = "${project.version}";
  /* The latest git revision of at the time of building**/
  public static final String REVISION = "${git.revision}";

  private ProjectConstants() {} // prevent instantiation
}