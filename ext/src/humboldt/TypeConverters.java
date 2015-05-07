package humboldt;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyInteger;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.exceptions.RaiseException;
import org.jruby.javasupport.Java;
import org.jruby.javasupport.JavaUtil;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.ByteList;

import org.jcodings.specific.ASCIIEncoding;
import org.jcodings.specific.USASCIIEncoding;
import org.jcodings.specific.UTF8Encoding;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TypeConverters {
  public static void load(Ruby runtime) {
    RubyModule humboldtModule = runtime.defineModule("Humboldt");
    RubyModule typeConverterModule = runtime.defineModuleUnder("TypeConverter", humboldtModule);
    RubyClass binaryClass = runtime.defineClassUnder("Binary", runtime.getObject(), BINARY_ALLOCATOR, typeConverterModule);
    binaryClass.defineAnnotatedMethods(BinaryConverter.class);
    runtime.defineClassUnder("Encoded", binaryClass, BINARY_ALLOCATOR, typeConverterModule);
    RubyClass textClass = runtime.defineClassUnder("Text", runtime.getObject(), TEXT_ALLOCATOR, typeConverterModule);
    textClass.defineAnnotatedMethods(TextConverter.class);
    runtime.defineClassUnder("Json", textClass, TEXT_ALLOCATOR, typeConverterModule);
    RubyClass longClass = runtime.defineClassUnder("Long", runtime.getObject(), LONG_ALLOCATOR, typeConverterModule);
    longClass.defineAnnotatedMethods(LongConverter.class);
    RubyClass noneClass = runtime.defineClassUnder("None", runtime.getObject(), NONE_ALLOCATOR, typeConverterModule);
    noneClass.defineAnnotatedMethods(NoneConverter.class);
  }

  private static final ObjectAllocator BINARY_ALLOCATOR = new ObjectAllocator() {
    @Override
    public IRubyObject allocate(Ruby runtime, RubyClass klass) {
      return new BinaryConverter(runtime, klass);
    }
  };

  private static final ObjectAllocator TEXT_ALLOCATOR = new ObjectAllocator() {
    @Override
    public IRubyObject allocate(Ruby runtime, RubyClass klass) {
      return new TextConverter(runtime, klass);
    }
  };

  private static final ObjectAllocator LONG_ALLOCATOR = new ObjectAllocator() {
    @Override
    public IRubyObject allocate(Ruby runtime, RubyClass klass) {
      return new LongConverter(runtime, klass);
    }
  };

  private static final ObjectAllocator NONE_ALLOCATOR = new ObjectAllocator() {
    @Override
    public IRubyObject allocate(Ruby runtime, RubyClass klass) {
      return new NoneConverter(runtime, klass);
    }
  };

  private static final RaiseException newTypeError(final Ruby runtime, final RubyClass actual, final String expected) {
    return runtime.newTypeError("Hadoop type mismatch, was " + actual.to_s() + ", expected " + expected);
  }

  private static final Object unwrapJavaObject(final IRubyObject arg) {
    if(JavaUtil.isJavaObject(arg)) {
      return JavaUtil.unwrapJavaObject(arg);
    } else {
      return null;
    }
  }

  @JRubyClass(name="Humboldt::TypeConverter::Binary")
  public static class BinaryConverter extends RubyObject {
    private BytesWritable value = new BytesWritable();

    public BinaryConverter(Ruby runtime, RubyClass klass) {
      super(runtime, klass);
    }

    @JRubyMethod(name="hadoop")
    public IRubyObject hadoop(final ThreadContext ctx) {
      return Java.getInstance(ctx.runtime, value);
    }

    @JRubyMethod(name="hadoop=")
    public IRubyObject hadoop_set(final ThreadContext ctx, IRubyObject arg) {
      Object object = unwrapJavaObject(arg);
      if (object == null) {
        throw newTypeError(ctx.runtime, arg.getMetaClass(), "Hadoop::Io::BytesWritable");
      } else {
        try {
          value = (BytesWritable)object;
        } catch (ClassCastException cce) {
          throw newTypeError(ctx.runtime, arg.getMetaClass(), "Hadoop::Io::BytesWritable");
        }
      }
      return arg;
    }

    @JRubyMethod(name="ruby")
    public IRubyObject ruby(final ThreadContext ctx) {
      RubyString string = RubyString.newString(ctx.runtime, value.getBytes(), 0, value.getLength());
      string.setEncoding(ASCIIEncoding.INSTANCE);
      return string;
    }

    @JRubyMethod(name = "ruby=", required = 1)
    public IRubyObject ruby_set(final ThreadContext ctx, IRubyObject arg) {
      RubyString string;
      try {
        string = arg.convertToString();
      } catch (RaiseException re) {
        throw newTypeError(ctx.runtime, arg.getMetaClass(), "String");
      }
      ByteList bytes = string.getByteList();
      value.set(bytes.getUnsafeBytes(), bytes.getBegin(), bytes.getRealSize());
      return arg;
    }
  }

  @JRubyClass(name="Humboldt::TypeConverter::Text")
  public static class TextConverter extends RubyObject {
    Text value = new Text();

    public TextConverter(Ruby runtime, RubyClass klass) {
      super(runtime, klass);
    }

    @JRubyMethod(name="hadoop")
    public IRubyObject hadoop(final ThreadContext ctx) {
      return Java.getInstance(ctx.runtime, value);
    }

    @JRubyMethod(name="hadoop=")
    public IRubyObject hadoop_set(final ThreadContext ctx, IRubyObject arg) {
      Object object = unwrapJavaObject(arg);
      if (object == null) {
        throw newTypeError(ctx.runtime, arg.getMetaClass(), "Hadoop::Io::Text");
      } else {
        try {
          value = (Text)object;
        } catch (ClassCastException cce) {
          throw newTypeError(ctx.runtime, arg.getMetaClass(), "Hadoop::Io::Text");
        }
      }
      return arg;
    }

    @JRubyMethod(name="ruby")
    public IRubyObject ruby(final ThreadContext ctx) {
      RubyString string = RubyString.newString(ctx.runtime, value.getBytes(), 0, value.getLength());
      string.setEncoding(UTF8Encoding.INSTANCE);
      return string;
    }

    @JRubyMethod(name="ruby=")
    public IRubyObject ruby_set(final ThreadContext ctx, IRubyObject arg) {
      RubyString string;
      try {
        string = arg.convertToString();
      } catch (RaiseException re) {
        throw newTypeError(ctx.runtime, arg.getMetaClass(), "String");
      }
      if (string.getEncoding() == UTF8Encoding.INSTANCE || string.getEncoding() == USASCIIEncoding.INSTANCE) {
        ByteList bytes = string.getByteList();
        value.set(bytes.getUnsafeBytes(), bytes.getBegin(), bytes.getRealSize());
      } else {
        value.set(string.toString());
      }
      return arg;
    }
  }

  @JRubyClass(name="Humboldt::TypeConverter::Long")
  public static class LongConverter extends RubyObject {
    private LongWritable value = new LongWritable();

    public LongConverter(Ruby runtime, RubyClass klass) {
      super(runtime, klass);
    }

    @JRubyMethod(name="hadoop")
    public IRubyObject hadoop(final ThreadContext ctx) {
      return Java.getInstance(ctx.runtime, value);
    }

    @JRubyMethod(name="hadoop=")
    public IRubyObject hadoop_set(final ThreadContext ctx, IRubyObject arg) {
      Object object = unwrapJavaObject(arg);
      if (object == null) {
        throw newTypeError(ctx.runtime, arg.getMetaClass(), "Hadoop::Io::LongWritable");
      } else {
        try {
          value = (LongWritable)object;
        } catch (ClassCastException cce) {
          throw newTypeError(ctx.runtime, arg.getMetaClass(), "Hadoop::Io::LongWritable");
        }
      }
      return arg;
    }

    @JRubyMethod(name="ruby")
    public IRubyObject ruby(final ThreadContext ctx) {
      return ctx.runtime.newFixnum(value.get());
    }

    @JRubyMethod(name="ruby=")
    public IRubyObject ruby_set(final ThreadContext ctx, IRubyObject arg) {
      long temp = 0;
      try {
        temp = ((RubyInteger)arg).getLongValue();
      } catch (ClassCastException cce) {
        throw newTypeError(ctx.runtime, arg.getMetaClass(), "Fixnum");
      } catch (RaiseException re) {
        throw newTypeError(ctx.runtime, arg.getMetaClass(), "Fixnum");
      }
      value.set(temp);
      return arg;
    }
  }

  @JRubyClass(name="Humboldt::TypeConverter::None")
  public static class NoneConverter extends RubyObject {
    public NoneConverter(Ruby runtime, RubyClass klass) {
      super(runtime, klass);
    }

    @JRubyMethod(name="hadoop")
    public IRubyObject hadoop(final ThreadContext ctx) {
      return Java.getInstance(ctx.runtime, NullWritable.get());
    }

    @JRubyMethod(name="hadoop=")
    public IRubyObject hadoop_set(final ThreadContext ctx, IRubyObject arg) {
      Object object = unwrapJavaObject(arg);
      if (object == null || !(object instanceof NullWritable)) {
        throw newTypeError(ctx.runtime, arg.getMetaClass(), "Hadoop::Io::NullWritable");
      }
      return arg;
    }

    @JRubyMethod(name="ruby")
    public IRubyObject ruby(final ThreadContext ctx) {
      return ctx.runtime.getNil();
    }

    @JRubyMethod(name="ruby=")
    public IRubyObject ruby_set(final ThreadContext ctx, IRubyObject arg) {
      if (!arg.isNil()) {
        throw newTypeError(ctx.runtime, arg.getMetaClass(), "NilClass");
      }
      return arg;
    }
  }
}
