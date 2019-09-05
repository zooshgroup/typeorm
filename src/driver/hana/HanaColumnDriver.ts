import {Driver} from "../Driver";
import {ObjectLiteral} from "../../common/ObjectLiteral";
import {DriverPackageNotInstalledError} from "../../error/DriverPackageNotInstalledError";
import {ColumnMetadata} from "../../metadata/ColumnMetadata";
import {HanaColumnQueryRunner} from "./HanaColumnQueryRunner";
import {DateUtils} from "../../util/DateUtils";
import {PlatformTools} from "../../platform/PlatformTools";
import {Connection} from "../../connection/Connection";
import {RdbmsSchemaBuilder} from "../../schema-builder/RdbmsSchemaBuilder";
import {HanaConnectionOptions} from "./HanaConnectionOptions";
import {MappedColumnTypes} from "../types/MappedColumnTypes";
import {ColumnType} from "../types/ColumnTypes";
import {QueryRunner} from "../../query-runner/QueryRunner";
import {DataTypeDefaults} from "../types/DataTypeDefaults";
import {TableColumn} from "../../schema-builder/table/TableColumn";
import {EntityMetadata} from "../../metadata/EntityMetadata";
import {ApplyValueTransformers} from "../../util/ApplyValueTransformers";

/**
 * Organizes communication with PostgreSQL DBMS.
 */
export class HanaColumnDriver implements Driver {

    // -------------------------------------------------------------------------
    // Public Properties
    // -------------------------------------------------------------------------

    /**
     * Connection used by driver.
     */
    connection: Connection;

        /**
     * Real database connection with sqlite database.
     */
    databaseConnection: any;

    /**
     * Hana underlying library.
     */
    hanaClient: any;

    /**
     * We store all created query runners because we need to release them.
     */
    connectedQueryRunners: QueryRunner[] = [];

    // -------------------------------------------------------------------------
    // Public Implemented Properties
    // -------------------------------------------------------------------------

    /**
     * Connection options.
     */
    options: HanaConnectionOptions;

    /**
     * Indicates if replication is enabled.
     */
    isReplicated = false;

    /**
     * Indicates if tree tables are supported by this driver.
     */
    treeSupport = true;

    /**
     * Gets list of supported column data types by a driver.
     *
     * @see https://www.tutorialspoint.com/postgresql/postgresql_data_types.htm
     * @see https://www.postgresql.org/docs/9.2/static/datatype.html
     */
    supportedDataTypes: ColumnType[] = [
        "boolean",
        "tinyint",
        "smallint",
        "integer",
        "bigint",
        "decimal", // DECIMAL(p,s)
        "real",
        "double",
        "varchar", // VARCHAR(n)
        "nvarchar", // NVARCHAR(n)
        "varbinary", // VARBINARY(n)
        "date",
        "time", // TIME(p)
        "timestamp", // TIMESTAMP(p)
        "blob",
        "clob",
        "nclob",
        "text"
    ];

    /**
     * Gets list of spatial column data types.
     */
    spatialTypes: ColumnType[] = [
    ];

    /**
     * Gets list of column data types that support length by a driver.
     */
    withLengthColumnTypes: ColumnType[] = [
        "varchar",
        "nvarchar",
        "varbinary"
    ];

    /**
     * Gets list of column data types that support precision by a driver.
     */
    withPrecisionColumnTypes: ColumnType[] = [
        "decimal",
        "time",
        "timestamp"
    ];

    /**
     * Gets list of column data types that support scale by a driver.
     */
    withScaleColumnTypes: ColumnType[] = [
        "decimal",
        "timestamp"
    ];

    /**
     * Orm has special columns and we need to know what database column types should be for those types.
     * Column types are driver dependant.
     */
    mappedDataTypes: MappedColumnTypes = {
        createDate: "timestamp",
        createDateDefault: "CURRENT_UTCTIMESTAMP",
        updateDate: "timestamp",
        updateDateDefault: "CURRENT_UTCTIMESTAMP",
        version: "integer",
        treeLevel: "integer",
        migrationId: "integer",
        migrationName: "nvarchar",
        migrationTimestamp: "integer",
        cacheId: "integer",
        cacheIdentifier: "nvarchar",
        cacheTime: "bigint",
        cacheDuration: "integer",
        cacheQuery: "nvarchar(5000)" as any,
        cacheResult: "clob",
        metadataType: "nvarchar",
        metadataDatabase: "nvarchar",
        metadataSchema: "nvarchar",
        metadataTable: "nvarchar",
        metadataName: "nvarchar",
        metadataValue: "clob",
    };

    /**
     * Default values of length, precision and scale depends on column data type.
     * Used in the cases when length/precision/scale is not specified by user.
     */
    dataTypeDefaults: DataTypeDefaults = {
        "decimal": { precision: 10, scale: 0 },
        "varchar": { length: 255 },
        "nvarchar": { length: 255 },
        "varbinary": { length: 2000 },
        "time": { precision: 0 },
        "timestamp": { precision: 5 },
    };

    /**
     * Max length allowed by Postgres for aliases.
     * @see https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
     */
    maxAliasLength = 63;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(connection: Connection) {
        this.connection = connection;
        this.options = connection.options as HanaConnectionOptions;

        // load hana package
        this.loadDependencies();
    }

    // -------------------------------------------------------------------------
    // Public Implemented Methods
    // -------------------------------------------------------------------------

    /**
     * Performs connection to the database.
     * Based on pooling options, it can either create connection immediately,
     * either create a pool and create connection when needed.
     */
    async connect(): Promise<void> {
        this.databaseConnection = await new Promise((ok, fail) => {
            const client=this.hanaClient.createConnection();


            const params = {
                UID: this.options.username,
                PWD: this.options.password,
                HOST: this.options.host,
                PORT: this.options.port,
                CURRENTSCHEMA: this.options.schema
            }

            client.connect(params,(err: any) => {
                if (err) return fail(err);
                client.setAutoCommit(false);
                ok(client);
            });

        });
    }

    /**
     * Makes any action after connection (e.g. create extensions in Postgres driver).
     */
    async afterConnect(): Promise<void> {
        return Promise.resolve();
    }

    /**
     * Closes connection with database.
     */
    async disconnect(): Promise<void> {
       // TODO
    }

    /**
     * Creates a schema builder used to build and sync a schema.
     */
    createSchemaBuilder() {
        return new RdbmsSchemaBuilder(this.connection);
    }

    /**
     * Creates a query runner used to execute database queries.
     */
    createQueryRunner() {
        return new HanaColumnQueryRunner(this);
    }

    /**
     * Prepares given value to a value to be persisted, based on its column type and metadata.
     */
    preparePersistentValue(value: any, columnMetadata: ColumnMetadata): any {

        if (columnMetadata.transformer)
            value = ApplyValueTransformers.transformTo(columnMetadata.transformer, value);

        if (value === null || value === undefined)
            return value;

        if (columnMetadata.type === Boolean) {
            return value === true ? 1 : 0;

        } else if (columnMetadata.type === "date") {
            return DateUtils.mixedDateToDateString(value);

        } else if (columnMetadata.type === "time") {
            return DateUtils.mixedDateToTimeString(value);

        } else if (columnMetadata.type === "datetime"
            || columnMetadata.type === Date
            || columnMetadata.type === "timestamp"
            || columnMetadata.type === "timestamp with time zone"
            || columnMetadata.type === "timestamp without time zone") {
            return DateUtils.mixedDateToDate(value).toISOString();

        } else if (["json", "jsonb", ...this.spatialTypes].indexOf(columnMetadata.type) >= 0) {
            return JSON.stringify(value);

        } else if (columnMetadata.type === "hstore") {
            if (typeof value === "string") {
                return value;
            } else {
                return Object.keys(value).map(key => {
                    return `"${key}"=>"${value[key]}"`;
                }).join(", ");
            }

        } else if (columnMetadata.type === "simple-array") {
            return DateUtils.simpleArrayToString(value);

        } else if (columnMetadata.type === "simple-json") {
            return DateUtils.simpleJsonToString(value);

        } else if (columnMetadata.type === "cube") {
            return `(${value.join(", ")})`;

        } else if (
            (
                columnMetadata.type === "enum"
                || columnMetadata.type === "simple-enum"
            )
            && !columnMetadata.isArray
        ) {
            return "" + value;
        }

        return value;
    }

    /**
     * Prepares given value to a value to be persisted, based on its column type or metadata.
     */
    prepareHydratedValue(value: any, columnMetadata: ColumnMetadata): any {
        if (value === null || value === undefined)
            return columnMetadata.transformer ? ApplyValueTransformers.transformFrom(columnMetadata.transformer, value) : value;

        if (columnMetadata.type === Boolean) {
            value = value ? true : false;

        } else if (columnMetadata.type === "datetime"
            || columnMetadata.type === Date
            || columnMetadata.type === "timestamp"
            || columnMetadata.type === "timestamp with time zone"
            || columnMetadata.type === "timestamp without time zone") {
            value = DateUtils.normalizeHydratedDate(value);

        } else if (columnMetadata.type === "date") {
            value = DateUtils.mixedDateToDateString(value);

        } else if (columnMetadata.type === "time") {
            value = DateUtils.mixedTimeToString(value);

        } else if (columnMetadata.type === "hstore") {
            if (columnMetadata.hstoreType === "object") {
                const regexp = /"(.*?)"=>"(.*?[^\\"])"/gi;
                const matchValue = value.match(regexp);
                const object: ObjectLiteral = {};
                let match;
                while (match = regexp.exec(matchValue)) {
                    object[match[1].replace(`\\"`, `"`)] = match[2].replace(`\\"`, `"`);
                }
                return object;

            } else {
                return value;
            }

        } else if (columnMetadata.type === "simple-array") {
            value = DateUtils.stringToSimpleArray(value);

        } else if (columnMetadata.type === "simple-json") {
            value = DateUtils.stringToSimpleJson(value);

        } else if (columnMetadata.type === "cube") {
            value = value.replace(/[\(\)\s]+/g, "").split(",").map(Number);

        } else if (columnMetadata.type === "enum" || columnMetadata.type === "simple-enum" ) {
            if (columnMetadata.isArray) {
                // manually convert enum array to array of values (pg does not support, see https://github.com/brianc/node-pg-types/issues/56)
                value = value !== "{}" ? (value as string).substr(1, (value as string).length - 2).split(",") : [];
                // convert to number if that exists in poosible enum options
                value = value.map((val: string) => {
                    return !isNaN(+val) && columnMetadata.enum!.indexOf(parseInt(val)) >= 0 ? parseInt(val) : val;
                });
            } else {
                // convert to number if that exists in poosible enum options
                value = !isNaN(+value) && columnMetadata.enum!.indexOf(parseInt(value)) >= 0 ? parseInt(value) : value;
            }
        }

        if (columnMetadata.transformer)
            value = ApplyValueTransformers.transformFrom(columnMetadata.transformer, value);

        return value;
    }

    /**
     * Replaces parameters in the given sql with special escaping character
     * and an array of parameter names to be passed to a query.
     */
    escapeQueryWithParameters(sql: string, parameters: ObjectLiteral, nativeParameters: ObjectLiteral): [string, any[]] {
        const builtParameters: any[] = Object.keys(nativeParameters).map(key => nativeParameters[key]);
        if (!parameters || !Object.keys(parameters).length)
            return [sql, builtParameters];

        const keys = Object.keys(parameters).map(parameter => "(:(\\.\\.\\.)?" + parameter + "\\b)").join("|");
        sql = sql.replace(new RegExp(keys, "g"), (key: string): string => {
            let value: any;
            let isArray = false;
            if (key.substr(0, 4) === ":...") {
                isArray = true;
                value = parameters[key.substr(4)];
            } else {
                value = parameters[key.substr(1)];
            }

            if (isArray) {
                return value.map((v: any) => {
                    builtParameters.push(v);
                    return "$" + builtParameters.length;
                }).join(", ");

            } else if (value instanceof Function) {
                return value();

            } else {
                builtParameters.push(value);
                return "$" + builtParameters.length;
            }
        }); // todo: make replace only in value statements, otherwise problems
        return [sql, builtParameters];
    }

    /**
     * Escapes a column name.
     */
    escape(columnName: string): string {
        return "\"" + columnName + "\"";
    }

    /**
     * Build full table name with schema name and table name.
     * E.g. "mySchema"."myTable"
     */
    buildTableName(tableName: string, schema?: string): string {
        return schema ? `${schema}.${tableName}` : tableName;
    }

    /**
     * Remove with schema name to get the table name
     * E.g. "mySchema"."myTable"
     */
    getShortTableName(fullTableName: string, schema?: string): string {
        return schema && fullTableName.startsWith(schema + ".") ? fullTableName.substring(schema.length + 1) : fullTableName;
    }

    /**
     * Creates a database type from a given column metadata.
     */
    normalizeType(column: { type?: ColumnType, length?: number | string, precision?: number|null, scale?: number, isArray?: boolean }): string {
        if (column.type === Number || column.type === "int") {
            return "integer";

        } else if (column.type === String || column.type === "varchar" || column.type === "uuid") {
            return "nvarchar";

        } else if (column.type === Date) {
            return "timestamp";

        } else if (column.type === Boolean) {
            return "boolean";

        } else {
            return column.type as string || "";
        }
    }

    /**
     * Normalizes "default" value of the column.
     */
    normalizeDefault(columnMetadata: ColumnMetadata): string {
        const defaultValue = columnMetadata.default;
        const arrayCast = columnMetadata.isArray ? `::${columnMetadata.type}[]` : "";

        if (
            (
                columnMetadata.type === "enum"
                || columnMetadata.type === "simple-enum"
            ) && defaultValue !== undefined
        ) {
            if (columnMetadata.isArray && Array.isArray(defaultValue)) {
                return `'{${defaultValue.map((val: string) => `${val}`).join(",")}}'`;
            }
            return `'${defaultValue}'`;
        }

        if (typeof defaultValue === "number") {
            return "" + defaultValue;

        } else if (typeof defaultValue === "boolean") {
            return defaultValue === true ? "true" : "false";

        } else if (typeof defaultValue === "function") {
            return defaultValue();

        } else if (typeof defaultValue === "string") {
            return `'${defaultValue}'${arrayCast}`;

        } else if (defaultValue === null) {
            return `null`;

        } else if (typeof defaultValue === "object") {
            return `'${JSON.stringify(defaultValue)}'`;

        } else {
            return defaultValue;
        }
    }

    /**
     * Normalizes "isUnique" value of the column.
     */
    normalizeIsUnique(column: ColumnMetadata): boolean {
        return column.entityMetadata.uniques.some(uq => uq.columns.length === 1 && uq.columns[0] === column);
    }

    /**
     * Returns default column lengths, which is required on column creation.
     */
    getColumnLength(column: ColumnMetadata|TableColumn): string {
        if (column.length)
            return column.length.toString();

        if (column.generationStrategy === "uuid")
            return "36";

        switch (column.type) {
            case String:
            case "varchar":
            case "nvarchar":
                return "255";
            case "varbinary":
                return "2000";
            default:
                return "";
        }
    }

    /**
     * Creates column type definition including length, precision and scale
     */
    createFullType(column: TableColumn): string {
        let type = column.type;

        // used 'getColumnLength()' method, because in Oracle column length is required for some data types.
        if (this.getColumnLength(column)) {
            type += `(${this.getColumnLength(column)})`;

        } else if (column.precision !== null && column.precision !== undefined && column.scale !== null && column.scale !== undefined) {
            type += "(" + column.precision + "," + column.scale + ")";

        } else if (column.precision !== null && column.precision !== undefined) {
            type += "(" + column.precision + ")";
        }
        return type;
    }

    /**
     * Obtains a new database connection to a master server.
     * Used for replication.
     * If replication is not setup then returns default connection's database connection.
     */
    obtainMasterConnection(): Promise<any> {
        return Promise.resolve();
    }

    /**
     * Obtains a new database connection to a slave server.
     * Used for replication.
     * If replication is not setup then returns master (default) connection's database connection.
     */
    obtainSlaveConnection(): Promise<any> {
        return Promise.resolve();
    }

   /**
     * Creates generated map of values generated or returned by database after INSERT query.
     */
    createGeneratedMap(metadata: EntityMetadata, insertResult: any) {
        return undefined;
    }

    /**
     * Differentiate columns of this table and columns from the given column metadatas columns
     * and returns only changed.
     */
    findChangedColumns(tableColumns: TableColumn[], columnMetadatas: ColumnMetadata[]): ColumnMetadata[] {
        // TODO (from Oracle)
        return columnMetadatas.filter(columnMetadata => {
            const tableColumn = tableColumns.find(c => c.name === columnMetadata.databaseName);
            if (!tableColumn)
                return false; // we don't need new columns, we only need exist and changed

            return tableColumn.name !== columnMetadata.databaseName
                || tableColumn.type !== this.normalizeType(columnMetadata)
                || tableColumn.length !== columnMetadata.length
                || tableColumn.precision !== columnMetadata.precision
                || tableColumn.scale !== columnMetadata.scale
                // || tableColumn.comment !== columnMetadata.comment || // todo
                || this.normalizeDefault(columnMetadata) !== tableColumn.default
                || tableColumn.isPrimary !== columnMetadata.isPrimary
                || tableColumn.isNullable !== columnMetadata.isNullable
                || tableColumn.isUnique !== this.normalizeIsUnique(columnMetadata)
                || (columnMetadata.generationStrategy !== "uuid" && tableColumn.isGenerated !== columnMetadata.isGenerated);
        });
    }
    
    /**
     * Returns true if driver supports RETURNING / OUTPUT statement.
     */
    isReturningSqlSupported(): boolean {
        return false;
    }

    /**
     * Returns true if driver supports uuid values generation on its own.
     */
    isUUIDGenerationSupported(): boolean {
        return false;
    }

    get uuidGenerator(): string {
        return "SELECT SYSUUID FROM DUMMY";
    }

    /**
     * Creates an escaped parameter.
     */
    createParameter(parameterName: string, index: number): string {
        return "$" + (index + 1);
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * If driver dependency is not given explicitly, then try to load it via "require".
     */
    protected loadDependencies(): void {
        try {
            this.hanaClient = PlatformTools.load("@sap/hana-client");
        } catch (e) { // todo: better error for browser env
            throw new DriverPackageNotInstalledError("HANA", "@sap/hana-client");
        }
    }

}
