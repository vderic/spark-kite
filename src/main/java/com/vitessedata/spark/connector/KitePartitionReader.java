package com.vitessedata.spark.connector;

import com.opencsv.CSVReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import com.vitessedata.kite.sdk.*;

public class KitePartitionReader implements PartitionReader<InternalRow> {

    private final KiteInputPartition csvInputPartition;
    private final String path;
    private Iterator<String[]> iterator;
    private CSVReader csvReader;
    private List<Function> valueConverters;
    private final StructType schema;

    public KitePartitionReader(KiteInputPartition csvInputPartition, StructType schema, String path, FileSpec filespec,
            Aggregation aggregation, StructType requiredSchema) throws FileNotFoundException, URISyntaxException {
        this.csvInputPartition = csvInputPartition;
        this.path = path;
        this.schema = schema;
        this.valueConverters = ValueConverters.getConverters(schema);
        this.createCsvReader();
    }

    private void createCsvReader() throws URISyntaxException, FileNotFoundException {
        FileReader filereader;
        // URL resource = this.getClass().getClassLoader().getResource(this.fileName);
        // filereader = new FileReader(new File(resource.toURI()));
        filereader = new FileReader(new File(this.path));
        csvReader = new CSVReader(filereader);
        iterator = csvReader.iterator();
        iterator.next();
        Integer[] fragment = csvInputPartition.getFragment();
        String[] hosts = csvInputPartition.preferredLocations();
        System.err.println("CreateCSvReader: fragid=" + fragment[0] + ", fragcnt = " + fragment[1]);

    }

    @Override
    public boolean next() {
        return iterator.hasNext();
    }

    @Override
    public InternalRow get() {
        Object[] values = iterator.next();
        Object[] convertedValues = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            convertedValues[i] = valueConverters.get(i).apply(values[i]);
        }
        return InternalRow.apply(
                JavaConverters.asScalaIteratorConverter(Arrays.asList(convertedValues).iterator()).asScala().toSeq());
    }

    @Override
    public void close() throws IOException {
        csvReader.close();
    }
}
