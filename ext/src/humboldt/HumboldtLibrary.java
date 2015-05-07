package humboldt;

public class HumboldtLibrary implements org.jruby.runtime.load.Library {
  public void load(final org.jruby.Ruby runtime, final boolean wrap) throws java.io.IOException {
    humboldt.TypeConverters.load(runtime);
  }
}
