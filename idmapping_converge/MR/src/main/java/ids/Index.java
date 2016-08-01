/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package ids;

import com.google.gson.Gson;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Index extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord, Writable, Cloneable {
  private Gson gson = new Gson();
  private static final long serialVersionUID = 2464611321926424530L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Index\",\"namespace\":\"ids\",\"fields\":[{\"name\":\"id\",\"type\":\"string\",\"avro.java.string\":\"String\"},{\"name\":\"global_id\",\"type\":\"string\",\"avro.java.string\":\"String\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public String id;
  @Deprecated public String global_id;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public Index() {}

  @Override
  protected Object clone() throws CloneNotSupportedException {
    Index index = new Index();
    index.setId(this.getId());
    index.setGlobalId(this.getGlobalId());
    return index;
  }

  /**
   * All-args constructor.
   * @param id The new value for id
   * @param global_id The new value for global_id
   */
  public Index(String id, String global_id) {
    this.id = id;
    this.global_id = global_id;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return id;
    case 1: return global_id;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: id = (String)value$; break;
    case 1: global_id = (String)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'id' field.
   * @return The value of the 'id' field.
   */
  public String getId() {
    return id;
  }

  /**
   * Sets the value of the 'id' field.
   * @param value the value to set.
   */
  public void setId(String value) {
    this.id = value;
  }

  /**
   * Gets the value of the 'global_id' field.
   * @return The value of the 'global_id' field.
   */
  public String getGlobalId() {
    return global_id;
  }

  /**
   * Sets the value of the 'global_id' field.
   * @param value the value to set.
   */
  public void setGlobalId(String value) {
    this.global_id = value;
  }

  /**
   * Creates a new Index RecordBuilder.
   * @return A new Index RecordBuilder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new Index RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Index RecordBuilder
   */
  public static Builder newBuilder(Builder other) {
    return new Builder(other);
  }

  /**
   * Creates a new Index RecordBuilder by copying an existing Index instance.
   * @param other The existing instance to copy.
   * @return A new Index RecordBuilder
   */
  public static Builder newBuilder(Index other) {
    return new Builder(other);
  }

  public void write(DataOutput dataOutput) throws IOException {
    byte[] bytes = this.toString().getBytes();
    dataOutput.writeInt(bytes.length);
    dataOutput.write(bytes, 0, bytes.length);
  }

  public void readFields(DataInput dataInput) throws IOException {
    int length = dataInput.readInt();
    byte[] bytes = new byte[length];
    dataInput.readFully(bytes, 0, length);
    Index index = (Index)this.gson.fromJson(new String(bytes), Index.class);
    this.setGlobalId(index.getGlobalId());
    this.setId(index.getId());
  }

  /**
   * RecordBuilder for Index instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Index>
    implements org.apache.avro.data.RecordBuilder<Index> {

    private String id;
    private String global_id;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.global_id)) {
        this.global_id = data().deepCopy(fields()[1].schema(), other.global_id);
        fieldSetFlags()[1] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing Index instance
     * @param other The existing instance to copy.
     */
    private Builder(Index other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.global_id)) {
        this.global_id = data().deepCopy(fields()[1].schema(), other.global_id);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'id' field.
      * @return The value.
      */
    public String getId() {
      return id;
    }

    /**
      * Sets the value of the 'id' field.
      * @param value The value of 'id'.
      * @return This builder.
      */
    public Builder setId(String value) {
      validate(fields()[0], value);
      this.id = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'id' field has been set.
      * @return True if the 'id' field has been set, false otherwise.
      */
    public boolean hasId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'id' field.
      * @return This builder.
      */
    public Builder clearId() {
      id = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'global_id' field.
      * @return The value.
      */
    public String getGlobalId() {
      return global_id;
    }

    /**
      * Sets the value of the 'global_id' field.
      * @param value The value of 'global_id'.
      * @return This builder.
      */
    public Builder setGlobalId(String value) {
      validate(fields()[1], value);
      this.global_id = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'global_id' field has been set.
      * @return True if the 'global_id' field has been set, false otherwise.
      */
    public boolean hasGlobalId() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'global_id' field.
      * @return This builder.
      */
    public Builder clearGlobalId() {
      global_id = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    public Index build() {
      try {
        Index record = new Index();
        record.id = fieldSetFlags()[0] ? this.id : (String) defaultValue(fields()[0]);
        record.global_id = fieldSetFlags()[1] ? this.global_id : (String) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  private static final org.apache.avro.io.DatumWriter
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  private static final org.apache.avro.io.DatumReader
    READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
