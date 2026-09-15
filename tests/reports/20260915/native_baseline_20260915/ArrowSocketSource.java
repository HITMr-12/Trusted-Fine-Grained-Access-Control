package fgac.bench;

import java.net.Socket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.io.IOException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.*;

/** Test-only local Arrow IPC source. No row conversion or result-file staging. */
public class ArrowSocketSource implements TableProvider {
  public StructType inferSchema(CaseInsensitiveStringMap opts) {
    return (StructType) DataType.fromJson(opts.get("schema"));
  }
  public boolean supportsExternalMetadata() { return true; }
  public Table getTable(StructType schema, Transform[] parts, Map<String,String> props) {
    return new ArrowTable(schema);
  }
  static class ArrowTable implements Table, SupportsRead {
    final StructType schema;
    ArrowTable(StructType s) { schema=s; }
    public String name() { return "BenchmarkArrowStream"; }
    public StructType schema() { return schema; }
    public Set<TableCapability> capabilities() { return EnumSet.of(TableCapability.BATCH_READ); }
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap opts) {
      return () -> new ArrowScan(schema, opts.get("ports"), opts.get("secret"));
    }
  }
  static class Part implements InputPartition {
    final int port; final String secret;
    Part(int p,String s) {port=p;secret=s;}
  }
  static class ArrowScan implements Scan, Batch {
    final StructType schema; final String ports,secret;
    ArrowScan(StructType s,String p,String k) {schema=s;ports=p;secret=k;}
    public StructType readSchema() {return schema;}
    public Batch toBatch() {return this;}
    public InputPartition[] planInputPartitions() {
      return Arrays.stream(ports.split(",")).map(x -> new Part(Integer.parseInt(x),secret))
        .toArray(InputPartition[]::new);
    }
    public PartitionReaderFactory createReaderFactory() {return new Factory();}
  }
  static class Factory implements PartitionReaderFactory {
    public boolean supportColumnarReads(InputPartition p) {return true;}
    public PartitionReader<InternalRow> createReader(InputPartition p) {
      throw new UnsupportedOperationException("Columnar reader required");
    }
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition p) {
      try {return new Reader((Part)p);} catch(IOException e) {throw new RuntimeException(e);}
    }
  }
  static class Reader implements PartitionReader<ColumnarBatch> {
    final Socket socket; final RootAllocator allocator; final ArrowStreamReader reader;
    ColumnarBatch batch;
    Reader(Part p) throws IOException {
      socket=new Socket();socket.connect(new InetSocketAddress("127.0.0.1",p.port),120000);
      socket.setSoTimeout(120000);
      socket.getOutputStream().write(p.secret.getBytes(StandardCharsets.US_ASCII));
      socket.getOutputStream().flush();allocator=new RootAllocator(128L*1024*1024);
      reader=new ArrowStreamReader(socket.getInputStream(),allocator);
    }
    public boolean next() throws IOException {
      if(!reader.loadNextBatch())return false;
      ColumnVector[] cols=reader.getVectorSchemaRoot().getFieldVectors().stream()
        .map(v -> new ArrowColumnVector(v) {public void close() { /* reader owns vectors */ }})
        .toArray(ColumnVector[]::new);
      batch=new ColumnarBatch(cols,reader.getVectorSchemaRoot().getRowCount());return true;
    }
    public ColumnarBatch get() {return batch;}
    public void close() throws IOException {
      try {reader.close();} finally {try {socket.close();} finally {allocator.close();}}
    }
  }
}
