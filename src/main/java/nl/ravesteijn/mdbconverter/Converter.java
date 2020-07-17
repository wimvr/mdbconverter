package nl.ravesteijn.mdbconverter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.IndexImpl;

public class Converter {
    private static final Logger LOG = LoggerFactory.getLogger(Converter.class);
    private static final String IMPORT_STATUS_TABLE = "mdb_import_status";
    
    private Connection mysqlConnection;
    private Database accessDb;
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
        if (Arrays.stream(args).anyMatch("-h"::equals) || Arrays.stream(args).anyMatch("--help"::equals)) {
            showHelpAndExit(0);
        }
        
        String mysqlUrl = null;
        String mysqlUser = null;
        String mysqlPassword = null;
        File mdbImport = null;
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-c":
                case "--config":
                    verifyParameterArgument("config", args, i);
                    Properties properties = new Properties();
                    properties.load(new FileInputStream(new File(args[++i])));
                    mysqlUrl = properties.getProperty("mysql.url");
                    mysqlUser = properties.getProperty("mysql.user");
                    mysqlPassword = properties.getProperty("mysql.password");
                    String mdbImportProp = properties.getProperty("mdb.import");
                    if (mdbImportProp != null) {
                        mdbImport = new File(mdbImportProp);
                    }
                    break;
                case "--mysqlurl":
                    verifyParameterArgument("mysqlurl", args, i);
                    mysqlUrl = args[++i];
                    break;
                case "--mysqluser":
                    verifyParameterArgument("mysqluser", args, i);
                    mysqlUser = args[++i];
                    break;
                case "--mysqlpassword":
                    verifyParameterArgument("mysqlpassword", args, i);
                    mysqlPassword = args[++i];
                    break;
                case "--mdb":
                    verifyParameterArgument("mdb", args, i);
                    mdbImport = new File(args[++i]);
                    break;
                default:
                    System.out.println("ERROR: Unknown parameter " + args[i]);
                    System.out.println("");
                    showHelpAndExit(1);
            }
        }
        
        verifyParameter("mysqlurl", mysqlUrl);
        verifyParameter("mysqluser", mysqlUser);
        verifyParameter("mysqlpassword", mysqlPassword);
        verifyParameter("mdb", mdbImport);
        
        new Converter(mysqlUrl, mysqlUser, mysqlPassword, mdbImport);
    }
    
    private static void showHelpAndExit(int exitCode) {
        System.out.println("Mdbconverter");
        System.out.println("");
        System.out.println("-h | --help:");
        System.out.println("\tShow this help");
        System.out.println("-c <file> | --config <file>");
        System.out.println("\tConfig file");
        System.out.println("--mysqlurl <url>");
        System.out.println("\tMySQL connect URL. Example: jdbc:mysql://localhost/<database>?useUnicode=true&characterEncoding=UTF-8&useSSL=false");
        System.out.println("--mysqluser <user>");
        System.out.println("\tMySQL user to connect with to the database");
        System.out.println("--mysqlpassword <password>");
        System.out.println("\tMySQL password to connect with to the database. WARNING: this is not secure, use config file instead.");
        System.out.println("--mdb <file>");
        System.out.println("\tMDB file to convert to MySQL");
        System.exit(exitCode);
    }
    
    private static void verifyParameterArgument(String name, String[] args, int index) {
        if (index + 1 >= args.length) {
            System.out.println("ERROR: Missing argument for parameter " + name);
            System.out.println("");
            showHelpAndExit(1);
        }
    }
    
    private static void verifyParameter(String name, String value) {
        if (value == null) {
            System.out.println("ERROR: " + name + " should be set.");
            System.out.println("");
            showHelpAndExit(1);
        }
    }
    
    private static void verifyParameter(String name, File value) {
        if (value == null) {
            System.out.println("ERROR: " + name + " should be set.");
            System.out.println("");
            showHelpAndExit(1);
        }
        if (!value.canRead()) {
            System.out.println("ERROR: cannot read file for " + name + ": " + value.getAbsolutePath());
            System.out.println("");
            showHelpAndExit(1);
        }
    }
    
    public Converter(String mysqlUrl, String mysqlUser, String mysqlPassword, File mdbImport) throws IOException, ClassNotFoundException, SQLException {
        LOG.info("MySQL URL: " + mysqlUrl);
        LOG.info("MySQL user: " + mysqlUser);
        LOG.info("MDB import: " + mdbImport.getAbsolutePath());
        StopWatch sw = StopWatch.createStarted();
        Class.forName("com.mysql.jdbc.Driver");
        
        createMysqlConnection(mysqlUrl, mysqlUser, mysqlPassword);
        accessDb = DatabaseBuilder.open(mdbImport);
        
        emptyMySQLDatabase();
        
        for (String tableName : accessDb.getTableNames()) {
            createTable(tableName);
        }
        for (String tableName : accessDb.getTableNames()) {
            copyData(tableName);
        }
        for (String tableName : accessDb.getTableNames()) {
            createConstraints(tableName);
        }
        storeStatus(mdbImport.getAbsoluteFile().getName(), sw.getTime(TimeUnit.SECONDS));
    }
    
    private void createTable(String tableName) throws IOException, SQLException {
        Table table = accessDb.getTable(tableName);
        List<String> columnDefs = table.getColumns().stream().map(c -> getColumnDefinition(c)).collect(Collectors.toList());
        columnDefs.add("PRIMARY KEY (`" + table.getPrimaryKeyIndex().getColumns().get(0).getName() + "`)");
        String sqlCreateTable = "CREATE TABLE `" + tableName + "` (\n\t" + StringUtils.join(columnDefs, ",\n\t")
                + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT=" + sqlEscapeValue((String) table.getProperties().getValue("Description")) + ";\n";
        LOG.info(sqlCreateTable);
        createStatement().execute(sqlCreateTable);
        LOG.info("Created table {}", tableName);
    }
    
    private String getColumnDefinition(Column column) {
        StringBuilder def = new StringBuilder("`" + column.getName() + "`");
        switch (column.getType()) {
            case BOOLEAN:
                def.append(" BOOLEAN");
                break;
            case BYTE:
                def.append(" CHAR(1)");
                break;
            case INT:
            case LONG:
                def.append(" INT(" + column.getLength() + ")");
                break;
            case MEMO:
                def.append(" TEXT");
                break;
            case TEXT:
                def.append(" VARCHAR(" + column.getLength() + ")");
                break;
            case SHORT_DATE_TIME:
                def.append(" DATE");
                break;
            case DOUBLE:
            case FLOAT:
            case MONEY:
                def.append(" FLOAT(" + column.getLength() + ")");
                break;
            default:
                throw new IllegalArgumentException(String.format("Unknown column type %s for %s.%s", column.getType(), column.getTable().getName(), column.getName()));
        }
        try {
            PropertyMap properties = column.getProperties();
            Boolean required = (Boolean) properties.getValue("Required");
            if (BooleanUtils.isTrue(required)) {
                def.append(" NOT NULL");
            }
            ColumnImpl columnImpl = (ColumnImpl) column;
            if (columnImpl.isAutoNumber()) {
                def.append(" AUTO_INCREMENT");
            }
            String defaultValue = (String) properties.getValue("DefaultValue");
            if (defaultValue != null) {
                String defaultString = convertValue(column.getType(), defaultValue);
                if (defaultString != null) {
                    def.append(" DEFAULT " + defaultString);
                }
            }
            String description = (String) properties.getValue("Description");
            if (description != null) {
                def.append(" COMMENT " + sqlEscapeValue(description));
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
        return def.toString();
    }
    
    private String convertValue(DataType dataType, Object value) {
        switch (dataType) {
            case BOOLEAN:
                if ("Yes".equals(value) || "True".equals(value) || "1".equals(value) || ((value instanceof Boolean) && ((Boolean) value).booleanValue())) {
                    return "TRUE";
                } else if ("No".equals(value) || "False".equals(value) || "0".equals(value) || ((value instanceof Boolean) && !((Boolean) value).booleanValue())) {
                    return "FALSE";
                } else {
                    throw new IllegalArgumentException(String.format("Cannot convert Boolean value '%s'", value));
                }
            case BYTE:
                if (value instanceof String) {
                    return sqlEscapeValue((String) value);
                } else {
                    return sqlEscapeValue(((Byte) value).toString());
                }
            case MEMO:
            case TEXT:
                return sqlEscapeValue((String) value);
            case FLOAT:
            case INT:
            case LONG:
                return String.valueOf(value);
            case SHORT_DATE_TIME:
                if (value instanceof Date) {
                    return sqlEscapeValue(new SimpleDateFormat("YYYY-MM-dd").format((Date) value));
                } else {
                    return null;
                }
            case DOUBLE:
            case MONEY:
                return "0";
            default:
                throw new IllegalArgumentException(String.format("Cannot convert value '%s' of type %s", value, dataType));
        }
    }
    
    private void copyData(String tableName) throws IOException, SQLException {
        Table table = accessDb.getTable(tableName);
        int totalRows = table.getRowCount();
        List<? extends Column> columns = table.getColumns();
        String insertSql = "INSERT INTO `" + tableName + "` (" + columns.stream().map(c -> c.getName()).collect(Collectors.joining("`, `", "`", "`")) + ") VALUES \n(";
        List<String> insertRowsSql = new ArrayList<>();
        Row row;
        int rowcount = 0;
        while ((row = table.getNextRow()) != null) {
            final Row rowX = row;
            rowcount++;
            insertRowsSql.add(columns.stream().map(c -> convertValue(c.getType(), rowX.get(c.getName()))).collect(Collectors.joining(", ")));
            if (insertRowsSql.size() >= 100) {
                String query = insertSql + StringUtils.join(insertRowsSql, "),\n(") + ");";
                LOG.info(String.format("[%s/%s] %s", rowcount, totalRows, query));
                createStatement().execute(query);
                insertRowsSql.clear();
            }
        }
        if (insertRowsSql.size() > 0) {
            String query = insertSql + StringUtils.join(insertRowsSql, "),\n(") + ");";
            LOG.info(String.format("[%s/%s] %s", rowcount, totalRows, query));
            createStatement().execute(query);
            insertRowsSql.clear();
        }
    }
    
    private void createConstraints(String tableName) throws IOException, SQLException {
        Table table = accessDb.getTable(tableName);
        List<String> constraintDefs = table.getIndexes().stream().map(i -> getConstraintDefinition(i)).filter(c -> c != null).collect(Collectors.toList());
        if (constraintDefs.size() > 0) {
            String sqlCreateTable = "ALTER TABLE `" + tableName + "`\n\t" + StringUtils.join(constraintDefs, ",\n\t") + ";";
            LOG.info(sqlCreateTable);
            createStatement().execute(sqlCreateTable);
            LOG.info("Created constraints for table {}", tableName);
        }
    }
    
    private String getConstraintDefinition(Index index) {
        if (index.isForeignKey() && index instanceof IndexImpl && !((IndexImpl) index).getReference().isPrimaryTable()) {
            try {
                return "ADD CONSTRAINT " + index.getName()
                        + " FOREIGN KEY (" + index.getColumns().stream().map(c -> "`" + c.getName() + "`").collect(Collectors.joining(","))
                        + ") REFERENCES `" + index.getReferencedIndex().getTable().getName() + "`(" + index.getReferencedIndex().getColumns().stream().map(c -> "`" + c.getName() + "`").collect(Collectors.joining(",")) + ")";
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
                return null;
            }
        } else {
            return null;
        }
    }
    
    private Statement createStatement() throws SQLException {
        return mysqlConnection.createStatement();
    }
    
    private void createMysqlConnection(String mysqlUrl, String mysqlUser, String mysqlPassword) throws SQLException {
        mysqlConnection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
    }
    
    private void emptyMySQLDatabase() throws SQLException {
        boolean hasImportStatusTable = false;
        Statement statement = createStatement();
        ResultSet rs = statement.executeQuery("SHOW TABLES");
        List<String> tables = new ArrayList<>();
        while (rs.next()) {
            String table = rs.getString(1);
            if (IMPORT_STATUS_TABLE.equals(table)) {
                hasImportStatusTable = true;
            } else {
                tables.add(table);
            }
        }
        if (tables.size() > 0) {
            createStatement().execute("SET FOREIGN_KEY_CHECKS = 0;");
            String deleteTablesSql = "DROP TABLE `" + StringUtils.join(tables, "`, `") + "`;";
            LOG.info(deleteTablesSql);
            createStatement().execute(deleteTablesSql);
            createStatement().execute("SET FOREIGN_KEY_CHECKS = 1;");
        }
        if (!hasImportStatusTable) {
            String sqlCreateTable = "CREATE TABLE `" + IMPORT_STATUS_TABLE + "` (`id` INT(4) NOT NULL AUTO_INCREMENT, `file` VARCHAR(255) NOT NULL, `importdate` DATETIME NOT NULL, `duration` INT(8) NOT NULL COMMENT 'Duration of import in seconds', PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='MDB Converter status';";
            LOG.info(sqlCreateTable);
            createStatement().execute(sqlCreateTable);
            LOG.info("Created table {}", IMPORT_STATUS_TABLE);
        }
    }
    
    private String sqlEscapeValue(String value) {
        if (value == null) {
            return "''";
        } else {
            return "'" + value.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'";
        }
    }
    
    private void storeStatus(String importedFile, long durationSeconds) throws SQLException {
        String insertSql = "INSERT INTO `" + IMPORT_STATUS_TABLE + "` (`file`, `importdate`, `duration`) VALUES (" + sqlEscapeValue(importedFile) + ", " + sqlEscapeValue(FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(new Date())) + ", " + durationSeconds + ");";
        LOG.info(insertSql);
        createStatement().execute(insertSql);
    }
}
