public class HumboldtService implements org.jruby.runtime.load.BasicLibraryService {
  public boolean basicLoad(final org.jruby.Ruby runtime) throws java.io.IOException {
    humboldt.TypeConverters.load(runtime);
    return true;
  }
}
