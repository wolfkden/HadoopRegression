package com.oviumzone;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.commons.math3.stat.regression.MillerUpdatingRegression;
import org.apache.commons.math3.stat.regression.RegressionResults;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.UNION;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Wolf 02/15/2015
 *
 * Runs data from HDFS stored as Avro files through MillerUpdatingRegression and reports the column weights.
 * If not given a Schema, defaults to first file specified.
 * ex:
 *
 * Presumes that non-numeric columns will be categories whose distinct members will fit in memory.
 * Maps numeric types to double per java conversion rules.
 * Maps BOOLEAN as TRUE -> 1.0, FALSE -> 0.0. 
 * Maps NULL to 0.0.
 * XXX Not Thread Safe.
 */
public class StreamingRegression {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingRegression.class);

    private final Collection<Path> paths = new LinkedList<Path>();
    private final Configuration configuration;
    private final String observedField;
    private final List<String> variables = new LinkedList<String>();
    private final Map<String, Map<Object, Integer>> fieldLookup = new HashMap<String, Map<Object, Integer>>();
    private final boolean scan;
    private final Schema schema;

    StreamingRegression(Configuration configuration, Schema schema, String observedField, boolean ignoreCategories) {
        this.configuration = configuration;
        this.schema = schema;
        this.observedField = observedField;
        boolean scan = false;
        long categoryFields = 0l;
        final Type observedType = schema.getField(observedField).schema().getType();
        if (!(
                DOUBLE.equals(observedType) ||
                        FLOAT.equals(observedType) ||
                        INT.equals(observedType) ||
                        LONG.equals(observedType) ||
                        BOOLEAN.equals(observedType) ||
                        NULL.equals(observedType)
        )) {
            throw new IllegalArgumentException("Target variable must be numeric.");
        }
        for (Field field : schema.getFields()) {
            final String name = field.name();
            final Type type = field.schema().getType();
            if (!observedField.equals(name)) {
                if (!(
                        DOUBLE.equals(type) ||
                                FLOAT.equals(type) ||
                                INT.equals(type) ||
                                LONG.equals(type) ||
                                BOOLEAN.equals(type) ||
                                NULL.equals(type)
                )) {
                    categoryFields++;
                    if (!ignoreCategories) {
                        fieldLookup.put(name, new HashMap<Object, Integer>());
                        scan = true;
                    }
                } else {
                    final Integer pos = variables.size();
                    variables.add(name);
                    fieldLookup.put(name, Collections.<Object, Integer>singletonMap(name, pos));
                }
            }
        }
        LOG.debug("Scan showed {} fields with non-numeric values, {}", categoryFields, scan ? "which we'll scan for category-based columns" : " pass --categories to turn them into category columns");
        this.scan = scan;
    }

    StreamingRegression(Configuration configuration, Schema schema, String observedField, boolean ignoreCategories, Path... paths) {
        this(configuration, schema, observedField, ignoreCategories);
        queuePaths(paths);
    }

    /**
     * Adds additional paths to be processed.
     */
    public void queuePaths(Path... paths) {
        this.paths.addAll(Arrays.asList(paths));
    }

    /**
     * Runs the streaming regression across queued files,
     * returns output of regressing over all files.
     */
    public RegressionResults run() throws IOException {
        final FileSystem fs = FileSystem.get(configuration);
        if (scan) {
            final int initialColumns = variables.size();
            LOG.info("Performing initial scan to determine category values.");
            long entries = 0l;
            for (Path path : paths) {
                final DataFileStream<GenericRecord> file = new DataFileStream<GenericRecord>(fs.open(path), new GenericDatumReader(schema));
                for (GenericRecord record : file) {
                    entries++;
                    final Schema recordSchema = record.getSchema();
                    for (Field field : recordSchema.getFields()) {
                        final Type type = field.schema().getType();
                        if (!(
                                DOUBLE.equals(type) ||
                                        FLOAT.equals(type) ||
                                        INT.equals(type) ||
                                        LONG.equals(type) ||
                                        BOOLEAN.equals(type) ||
                                        NULL.equals(type)
                        )) {
                            final String name = field.name();
                            final Object value = record.get(field.pos());
                            final Map<Object, Integer> lookup = fieldLookup.get(name);
                            if (!(lookup.containsKey(value))) {
                                final Integer pos = variables.size();
                                final String category = name + " is '" + value + "'";
                                variables.add(category);
                /* XXX Possible future optimization, Use category instead of value, so we don't keep references */
                                lookup.put(value, pos);
                            }
                        }
                    }
                }
            }
            LOG.info("Scan complete. Checked {} rows, added {} columns", entries, variables.size() - initialColumns);
        }

        final int numFields = variables.size();
        final MillerUpdatingRegression regression = new MillerUpdatingRegression(numFields,true);

        for (Path path : paths) {
            final DataFileStream<GenericRecord> file = new DataFileStream<GenericRecord>(fs.open(path), new GenericDatumReader(schema));
            for (GenericRecord record : file) {
                final Schema recordSchema = record.getSchema();
                final double[] x = new double[numFields];
                final double y = valueToComponent(record.get(observedField), recordSchema.getField(observedField).schema().getType());
                for (Field field : recordSchema.getFields()) {
                    final String name = field.name();
                    if(!(observedField.equals(name)) && fieldLookup.containsKey(name)) {
                        final Type type = field.schema().getType();
                        final Object value = record.get(field.pos());
                        switch(type) {
                            case DOUBLE:
                            case FLOAT:
                            case INT:
                            case LONG:
                            case BOOLEAN:
                            case NULL:
                                x[fieldLookup.get(name).get(name)] = valueToComponent(value, type);
                                break;
                            default:
              /* Category, pick a column */
                                x[fieldLookup.get(name).get(value)] = valueToComponent(value, type);
                        }
                    }
                }

                regression.addObservation(x, y);
            }
        }

        return regression.regress();
    }

    double valueToComponent(final Object value, final Type type) {
        double component;
        switch (type) {
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
                component = ((Number)value).doubleValue();
                break;
            case BOOLEAN:
                component = ((Boolean)value).booleanValue() ? 1.0 : 0.0;
                break;
            case NULL:
                component = 0.0;
                break;
            case UNION:
                throw new UnsupportedOperationException("Union handling not implemented");
            default:
                /* Must be a category, in that case the column is determined by the value and we always return 1.0 */
                component = 1.0;
        }
        return component;
    }

    public static void main(String[] args) throws IOException {

        final Options options = new Options();

        final Option field = new Option("t", "target", true, "The name of the column containing the variable the regression should target");
        field.setRequired(true);
        options.addOption(field);

        final Option files = new Option("f", "file", true, "An HDFS path glob for the files you want the regression to run on.");
        files.setRequired(true);
        options.addOption(files);

        options.addOption("s", "schema", true, "A path to a file containing an avro Schema to use for reading source files.");
        options.addOption("l", "local-schema", false, "flag to indicate schema path is in the local filesystem, rather than hdfs");
        //options.addOption("i", "ignore", true, "comma seperated list of field names to not include in the regression.");
        options.addOption("c", "categories", false, "flag to indicate you would like non-numeric fields handled as categories. WARNING: the set of distinct values must fit in memory.");

        final CommandLineParser parser = new GnuParser();
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
        } catch (ParseException exception) {
            (new HelpFormatter()).printHelp("hadoop jar streaming_linear_regression_from_hdfs-0.0.1-SNAPSHOT.jar", options, true);
            System.exit(-1);
        }

        final String observed = line.getOptionValue("target");
        final Configuration config = new Configuration();
        final FileSystem fs = FileSystem.get(config);

        final ArrayList<Path> paths = new ArrayList<Path>();

        for (FileStatus status : fs.globStatus(new Path(line.getOptionValue("file")))) {
            if (!status.isDir()) {
                paths.add(status.getPath());
            }
        }

        if (1 > paths.size()) {
            throw new IllegalArgumentException("Glob didn't match any files.");
        }

        Schema schema;

        if (line.hasOption("schema")) {
            InputStream input;
            final String path = line.getOptionValue("schema");
            if (line.hasOption("local-schema")) {
                input = new FileInputStream(path);
            } else {
                input = fs.open(new Path(path));
            }
            schema = (new Schema.Parser()).parse(input);
        } else {
            schema = (new DataFileStream<GenericRecord>(fs.open(paths.get(0)), new GenericDatumReader<GenericRecord>())).getSchema();
        }

        if (!(RECORD.equals(schema.getType()))) {
            throw new UnsupportedOperationException("First retrieved path has to be an avro file with records");
        }

        final StreamingRegression hsr = new StreamingRegression(config, schema, observed, !(line.hasOption("categories")), paths.toArray(new Path[0]));
        RegressionResults results = hsr.run();

        LOG.info("Results: {}", results);
        LOG.info("tacking variable {}", observed);
        final double[] estimates = results.getParameterEstimates();
        final double[] errors = results.getStdErrorOfEstimates();
        final List<String> variables = hsr.variables;
        LOG.info("Regression based on {} observations on {} variables:", results.getN(), estimates.length);
        LOG.info("R-squared: {}", String.format("%.4f", results.getRSquared()));
        LOG.info("SSE: {}", String.format("%+e", results.getErrorSumSquares()));
        LOG.info("MSE: {}", String.format("%+e", results.getMeanSquareError()));

        LOG.info("parameter estimates:");
        final int observedPos = schema.getField(observed).pos();
        int i = 0;
        int j = 0;
        if (results.hasIntercept()) {
            LOG.info("\tconstant used: {}", String.format("%+.4f", estimates[0]));
            i++;
        }

        final List<Pair> ordered = new LinkedList<Pair>();
        for (; i < estimates.length; i++, j++) {
            ordered.add(new Pair(estimates[i], variables.get(j), errors[i]));
        }

        Collections.sort(ordered);

        for (Pair pair : ordered) {
            LOG.info("\t{}\t{}", String.format("%+.4f\t:s(b_i): %+.4e", pair.value, pair.error), pair.column);
            //LOG.info("\t{}\t{}", String.format("%+.4f", pair.value, pair.error), pair.column);
        }
    }

    static class Pair implements Comparable<Pair> {
        final double value;
        final String column;
        final double error;

        Pair(Double value, String column, double error) {
            this.value = value;
            this.column = column;
            this.error = error;
        }

        public int compareTo(Pair other) {
            return Double.compare(Math.abs(other.value), Math.abs(value));

        }
    }

}
