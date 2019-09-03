import { QueryRunner } from "../../query-runner/QueryRunner";
import { TransactionAlreadyStartedError } from "../../error/TransactionAlreadyStartedError";
import { TransactionNotStartedError } from "../../error/TransactionNotStartedError";
import { TableColumn } from "../../schema-builder/table/TableColumn";
import { Table } from "../../schema-builder/table/Table";
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey";
import { TableIndex } from "../../schema-builder/table/TableIndex";
import { QueryRunnerAlreadyReleasedError } from "../../error/QueryRunnerAlreadyReleasedError";
import { View } from "../../schema-builder/view/View";
import { Query } from "../Query";
import { HanaColumnDriver } from "./HanaColumnDriver";
import { ReadStream } from "../../platform/PlatformTools";
import { QueryFailedError } from "../../error/QueryFailedError";
import { TableUnique } from "../../schema-builder/table/TableUnique";
import { BaseQueryRunner } from "../../query-runner/BaseQueryRunner";
import { TableCheck } from "../../schema-builder/table/TableCheck";
import { IsolationLevel } from "../types/IsolationLevel";
import { TableExclusion } from "../../schema-builder/table/TableExclusion";
import { Broadcaster } from "../../subscriber/Broadcaster";
import { OperationNotSupportedError } from '../../error/OperationNotSupportedError';
import { ObjectLiteral } from '../../common/ObjectLiteral';
import { ColumnType } from '../types/ColumnTypes';


/**
 * Runs queries on a single mysql database connection.
 */
export class HanaColumnQueryRunner extends BaseQueryRunner implements QueryRunner {

    // -------------------------------------------------------------------------
    // Public Implemented Properties
    // -------------------------------------------------------------------------

    /**
     * Database driver used by connection.
     */
    driver: HanaColumnDriver;

    // -------------------------------------------------------------------------
    // Protected Properties
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(driver: HanaColumnDriver) {
        super();
        this.driver = driver;
        this.connection = driver.connection;
        this.broadcaster = new Broadcaster(this);
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Creates/uses database connection from the connection pool to perform further operations.
     * Returns obtained database connection.
     */
    connect(): Promise<any> {
        return Promise.resolve(this.driver.databaseConnection);
    }

    /**
     * Releases used database connection.
     * We just clear loaded tables and sql in memory, because sqlite do not support multiple connections thus query runners.
     */
    release(): Promise<void> {
        this.loadedTables = [];
        this.clearSqlMemory();
        return Promise.resolve();
    }

    /**
     * Starts transaction on the current connection.
     */
    async startTransaction(isolationLevel?: IsolationLevel): Promise<void> {
        if (this.isTransactionActive)
            throw new TransactionAlreadyStartedError();


        this.isTransactionActive = true;
        if (isolationLevel) {
            if (isolationLevel !== "READ COMMITTED" && isolationLevel !== "REPEATABLE READ" && isolationLevel !== "SERIALIZABLE") {
                throw new OperationNotSupportedError(`HANA only supports SERIALIZABLE, READ COMMITTED and REPEATABLE READ isolation`);
            }

            await this.query("SET TRANSACTION ISOLATION LEVEL " + isolationLevel);
        }
    }

    /**
     * Commits transaction.
     * Error will be thrown if transaction was not started.
     */
    async commitTransaction(): Promise<void> {
        if (!this.isTransactionActive)
            throw new TransactionNotStartedError();

        this.driver.databaseConnection.commit();
        this.isTransactionActive = false;
    }

    /**
     * Rollbacks transaction.
     * Error will be thrown if transaction was not started.
     */
    async rollbackTransaction(): Promise<void> {
        if (!this.isTransactionActive)
            throw new TransactionNotStartedError();

        this.driver.databaseConnection.rollback();
        this.isTransactionActive = false;
    }

    /**
     * Executes a raw SQL query.
     */
    query(query: string, parameters?: any[]): Promise<any> {
        if (this.isReleased)
            throw new QueryRunnerAlreadyReleasedError();

        return new Promise(async (ok, fail) => {
            try {
                const databaseConnection = await this.connect();
                this.driver.connection.logger.logQuery(query, parameters, this);
                const queryStartTime = +new Date();
                databaseConnection.exec(query, parameters, (err: any, result: any) => {

                    // log slow queries if maxQueryExecution time is set
                    const maxQueryExecutionTime = this.driver.connection.options.maxQueryExecutionTime;
                    const queryEndTime = +new Date();
                    const queryExecutionTime = queryEndTime - queryStartTime;
                    if (maxQueryExecutionTime && queryExecutionTime > maxQueryExecutionTime)
                        this.driver.connection.logger.logQuerySlow(queryExecutionTime, query, parameters, this);

                    if (err) {
                        this.driver.connection.logger.logQueryError(err, query, parameters, this);
                        return fail(new QueryFailedError(query, parameters, err));
                    }

                    ok(result);
                });

            } catch (err) {
                fail(err);
            }
        });
    }

    /**
     * Returns raw data stream.
     */
    stream(query: string, parameters?: any[], onEnd?: Function, onError?: Function): Promise<ReadStream> {
        throw new OperationNotSupportedError();
    }

    /**
     * Returns all available database names including system databases.
     */
    async getDatabases(): Promise<string[]> {
        return Promise.resolve([]);
    }

    /**
     * Returns all available schema names including system schemas.
     * If database parameter specified, returns schemas of that database.
     */
    async getSchemas(database?: string): Promise<string[]> {
        throw new OperationNotSupportedError();
    }

    /**
     * Checks if database with the given name exist.
     */
    async hasDatabase(database: string): Promise<boolean> {
        throw new OperationNotSupportedError();
    }

    /**
     * Checks if schema with the given name exist.
     */
    async hasSchema(schema: string): Promise<boolean> {
        throw new OperationNotSupportedError();
    }

    /**
     * Checks if table with the given name exist in the database.
     */
    async hasTable(tableOrName: Table | string): Promise<boolean> {
        const currentSchema = await this.getCurrentSchema();

        const tableName = tableOrName instanceof Table ? tableOrName.name : `\"${tableOrName}\"`;
        const sql = `SELECT "TABLE_NAME" FROM "TABLES" WHERE "TABLE_NAME" = '${tableName}' AND SCHEMA_NAME = '${currentSchema}'`;
        const result = await this.query(sql);
        return result.length ? true : false;
    }

    /**
     * Checks if column with the given name exist in the given table.
     */
    async hasColumn(tableOrName: Table | string, column: TableColumn | string): Promise<boolean> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new database.
     */
    async createDatabase(database: string, ifNotExist?: boolean): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops database.
     */
    async dropDatabase(database: string, ifExist?: boolean): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new table schema.
     */
    async createSchema(schema: string, ifNotExist?: boolean): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops table schema.
     */
    async dropSchema(schemaPath: string, ifExist?: boolean): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new table.
     */
    async createTable(table: Table, ifNotExist: boolean = false, createForeignKeys: boolean = true, createIndices: boolean = true): Promise<void> {

        if (ifNotExist) {
            const isTableExist = await this.hasTable(table);
            if (isTableExist) return Promise.resolve();
        }
        const upQueries: Query[] = [];
        const downQueries: Query[] = [];

        upQueries.push(this.createTableSql(table, createForeignKeys));
        downQueries.push(this.dropTableSql(table));

        upQueries.push(this.createPrimaryKeySql(table, table.primaryColumns.map(column => column.name)));
        downQueries.push(this.dropPrimaryKeySql(table));

        // if createForeignKeys is true, we must drop created foreign keys in down query.
        // createTable does not need separate method to create foreign keys, because it create fk's in the same query with table creation.
        if (createForeignKeys)
            table.foreignKeys.forEach(foreignKey => downQueries.push(this.dropForeignKeySql(table, foreignKey)));

        if (createIndices) {
            table.indices.forEach(index => {
                // new index may be passed without name. In this case we generate index name manually.
                if (!index.name)
                    index.name = this.connection.namingStrategy.indexName(table.name, index.columnNames, index.where);
                upQueries.push(this.createIndexSql(table, index));
                downQueries.push(this.dropIndexSql(index));
            });
        }

        await this.executeQueries(upQueries, downQueries);
    }

    /**
     * Drop the table.
     */
    async dropTable(tableOrName: Table|string, ifExist?: boolean, dropForeignKeys: boolean = true, dropIndices: boolean = true): Promise<void> {// It needs because if table does not exist and dropForeignKeys or dropIndices is true, we don't need
        // to perform drop queries for foreign keys and indices.
        if (ifExist) {
            const isTableExist = await this.hasTable(tableOrName);
            if (!isTableExist) return Promise.resolve();
        }

        // if dropTable called with dropForeignKeys = true, we must create foreign keys in down query.
        const createForeignKeys: boolean = dropForeignKeys;
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const upQueries: Query[] = [];
        const downQueries: Query[] = [];


        if (dropIndices) {
            table.indices.forEach(index => {
                upQueries.push(this.dropIndexSql(index));
                downQueries.push(this.createIndexSql(table, index));
            });
        }

        // if dropForeignKeys is true, we just drop the table, otherwise we also drop table foreign keys.
        // createTable does not need separate method to create foreign keys, because it create fk's in the same query with table creation.
        if (dropForeignKeys)
            table.foreignKeys.forEach(foreignKey => upQueries.push(this.dropForeignKeySql(table, foreignKey)));

        upQueries.push(this.dropTableSql(table));
        downQueries.push(this.createTableSql(table, createForeignKeys));

        await this.executeQueries(upQueries, downQueries);
    }

    /**
     * Creates a new view.
     */
    async createView(view: View): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops the view.
     */
    async dropView(target: View | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Renames a table.
     */
    async renameTable(oldTableOrName: Table | string, newTableName: string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new column from the column in the table.
     */
    async addColumn(tableOrName: Table | string, column: TableColumn): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new columns from the column in the table.
     */
    async addColumns(tableOrName: Table | string, columns: TableColumn[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Renames column in the given table.
     */
    async renameColumn(tableOrName: Table | string, oldTableColumnOrName: TableColumn | string, newTableColumnOrName: TableColumn | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Changes a column in the table.
     */
    async changeColumn(tableOrName: Table|string, oldTableColumnOrName: TableColumn|string, newColumn: TableColumn): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Extracts schema name from given Table object or table name string.
     */
    protected extractSchema(target: Table|string): string|undefined {
        const tableName = target instanceof Table ? target.name : target;
        return tableName.indexOf(".") === -1 ? this.driver.options.schema : tableName.split(".")[0];
    }

    /**
     * Changes a column in the table.
     */
    async changeColumns(tableOrName: Table|string, changedColumns: { newColumn: TableColumn, oldColumn: TableColumn }[]): Promise<void> {
        return; //throw new OperationNotSupportedError();
    }
    
    /**
     * Drops column in the table.
     */
    async dropColumn(tableOrName: Table | string, columnOrName: TableColumn | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops the columns in the table.
     */
    async dropColumns(tableOrName: Table | string, columns: TableColumn[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new primary key.
     */
    async createPrimaryKey(tableOrName: Table | string, columnNames: string[]): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const clonedTable = table.clone();

        const up = this.createPrimaryKeySql(table, columnNames);

        // mark columns as primary, because dropPrimaryKeySql build constraint name from table primary column names.
        clonedTable.columns.forEach(column => {
            if (columnNames.find(columnName => columnName === column.name))
                column.isPrimary = true;
        });
        const down = this.dropPrimaryKeySql(clonedTable);

        await this.executeQueries(up, down);
        this.replaceCachedTable(table, clonedTable);
    }

    /**
     * Updates composite primary keys.
     */
    async updatePrimaryKeys(tableOrName: Table | string, columns: TableColumn[]): Promise<void> {
        return; //throw new OperationNotSupportedError();
    }

    /**
     * Drops a primary key.
     */
    async dropPrimaryKey(tableOrName: Table | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new unique constraint.
     */
    async createUniqueConstraint(tableOrName: Table | string, uniqueConstraint: TableUnique): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new unique constraints.
     */
    async createUniqueConstraints(tableOrName: Table | string, uniqueConstraints: TableUnique[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops an unique constraint.
     */
    async dropUniqueConstraint(tableOrName: Table | string, uniqueOrName: TableUnique | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops an unique constraints.
     */
    async dropUniqueConstraints(tableOrName: Table | string, uniqueConstraints: TableUnique[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new check constraint.
     */
    async createCheckConstraint(tableOrName: Table | string, checkConstraint: TableCheck): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);

        // new unique constraint may be passed without name. In this case we generate unique name manually.
        if (!checkConstraint.name)
            checkConstraint.name = this.connection.namingStrategy.checkConstraintName(table.name, checkConstraint.expression!);

        const up = this.createCheckConstraintSql(table, checkConstraint);
        const down = this.dropCheckConstraintSql(table, checkConstraint);
        await this.executeQueries(up, down);
        table.addCheckConstraint(checkConstraint);
    }

    /**
     * Builds create check constraint sql.
     */
    protected createCheckConstraintSql(table: Table, checkConstraint: TableCheck): Query {
        return new Query(`ALTER TABLE ${this.escapePath(table.name)} ADD CONSTRAINT "${checkConstraint.name}" CHECK (${checkConstraint.expression})`);
    }

    /**
     * Builds drop check constraint sql.
     */
    protected dropCheckConstraintSql(table: Table, checkOrName: TableCheck|string): Query {
        const checkName = checkOrName instanceof TableCheck ? checkOrName.name : checkOrName;
        return new Query(`ALTER TABLE ${this.escapePath(table.name)} DROP CONSTRAINT "${checkName}"`);
    }


    /**
     * Creates a new check constraints.
     */
    async createCheckConstraints(tableOrName: Table | string, checkConstraints: TableCheck[]): Promise<void> {
        const promises = checkConstraints.map(checkConstraint => this.createCheckConstraint(tableOrName, checkConstraint));
        await Promise.all(promises);
    }

    /**
     * Drops check constraint.
     */
    async dropCheckConstraint(tableOrName: Table | string, checkOrName: TableCheck | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops check constraints.
     */
    async dropCheckConstraints(tableOrName: Table | string, checkConstraints: TableCheck[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new exclusion constraint.
     */
    async createExclusionConstraint(tableOrName: Table | string, exclusionConstraint: TableExclusion): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new exclusion constraints.
     */
    async createExclusionConstraints(tableOrName: Table | string, exclusionConstraints: TableExclusion[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops exclusion constraint.
     */
    async dropExclusionConstraint(tableOrName: Table | string, exclusionOrName: TableExclusion | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops exclusion constraints.
     */
    async dropExclusionConstraints(tableOrName: Table | string, exclusionConstraints: TableExclusion[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new foreign key.
     */
    async createForeignKey(tableOrName: Table | string, foreignKey: TableForeignKey): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new foreign keys.
     */
    async createForeignKeys(tableOrName: Table | string, foreignKeys: TableForeignKey[]): Promise<void> {
        return; //throw new OperationNotSupportedError();
    }

    /**
     * Drops a foreign key.
     */
    async dropForeignKey(tableOrName: Table | string, foreignKeyOrName: TableForeignKey | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops a foreign keys from the table.
     */
    async dropForeignKeys(tableOrName: Table | string, foreignKeys: TableForeignKey[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new index.
     */
    async createIndex(tableOrName: Table|string, index: TableIndex): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);

        // new index may be passed without name. In this case we generate index name manually.
        if (!index.name)
            index.name = this.connection.namingStrategy.indexName(table.name, index.columnNames, index.where);

        const up = this.createIndexSql(table, index);
        const down = this.dropIndexSql(index);
        await this.executeQueries(up, down);
        table.addIndex(index);
    }

    /**
     * Creates a new indices
     */
    async createIndices(tableOrName: Table|string, indices: TableIndex[]): Promise<void> {
        const promises = indices.map(index => this.createIndex(tableOrName, index));
        await Promise.all(promises);
    }

    async dropIndex(tableOrName: Table|string, indexOrName: TableIndex|string): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const index = indexOrName instanceof TableIndex ? indexOrName : table.indices.find(i => i.name === indexOrName);
        if (!index)
            throw new Error(`Supplied index was not found in table ${table.name}`);

        const up = this.dropIndexSql(index);
        const down = this.createIndexSql(table, index);
        await this.executeQueries(up, down);
        table.removeIndex(index);
    }

    /**
     * Drops an indices from the table.
     */
    async dropIndices(tableOrName: Table|string, indices: TableIndex[]): Promise<void> {
        const promises = indices.map(index => this.dropIndex(tableOrName, index));
        await Promise.all(promises);
    }

    /**
     * Clears all table contents.
     * Note: this operation uses SQL's TRUNCATE query which cannot be reverted in transactions.
     */
    async clearTable(tableOrName: Table | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Removes all tables from the currently connected database.
     * Be careful using this method and avoid using it in production or migrations
     * (because it can clear all your database).
     */
    async clearDatabase(database?: string): Promise<void> {
        const currentSchema = await this.getCurrentSchema();

        await this.startTransaction();
        try {
            const dropViewsQuery = `SELECT 'DROP VIEW "' || SCHEMA_NAME || '"."' || VIEW_NAME || '"' AS "query" FROM "VIEWS" WHERE SCHEMA_NAME = '${currentSchema}'`;
            const dropViewQueries: ObjectLiteral[] = await this.query(dropViewsQuery);
            await Promise.all(dropViewQueries.map(query => this.query(query["query"])));

            const dropTablesQuery = `SELECT 'DROP TABLE "' || SCHEMA_NAME || '"."' || TABLE_NAME || '" CASCADE' AS "query" FROM "TABLES" WHERE SCHEMA_NAME = '${currentSchema}'`;
            const dropTableQueries: ObjectLiteral[] = await this.query(dropTablesQuery);
            await Promise.all(dropTableQueries.map(query => this.query(query["query"])));
            await this.commitTransaction();

        } catch (error) {
            try { // we throw original error even if rollback thrown an error
                await this.rollbackTransaction();
            } catch (rollbackError) { }
            throw error;
        }
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * Returns current database.
     */
    protected async getCurrentDatabase(): Promise<string> {
        throw new OperationNotSupportedError();
    }

    protected async loadViews(viewNames: string[]): Promise<View[]> {
        return [];
    }

    /**
     * Loads all tables (with given names) from the database and creates a Table from them.
     */
    protected async loadTables(tableNames: string[]): Promise<Table[]> {

        // if no tables given then no need to proceed
        if (!tableNames || !tableNames.length)
            return [];

        const currentSchema = await this.getCurrentSchema();

        // load tables, columns, indices and foreign keys
        const tableNamesString = tableNames.map(name => "'" + this.driver.getShortTableName(name, currentSchema) + "'").join(", ");
        const tablesSql = `SELECT * FROM "TABLES" WHERE "TABLE_NAME" IN (${tableNamesString}) AND "SCHEMA_NAME" = '${currentSchema}'`;
        const columnsSql = `SELECT * FROM "TABLE_COLUMNS" WHERE "TABLE_NAME" IN (${tableNamesString}) AND "SCHEMA_NAME" = '${currentSchema}'`;
        const constraintsSql = `SELECT * FROM "CONSTRAINTS" WHERE "TABLE_NAME" IN (${tableNamesString}) AND "SCHEMA_NAME" = '${currentSchema}'`;
        const indexesSql = `SELECT * FROM "INDEXES" WHERE "TABLE_NAME" IN (${tableNamesString}) AND "SCHEMA_NAME" = '${currentSchema}'`;
        const indexeColumnsSql = `SELECT * FROM "INDEX_COLUMNS" WHERE "TABLE_NAME" IN (${tableNamesString}) AND "SCHEMA_NAME" = '${currentSchema}'`;

        const [dbTables, dbColumns, dbConstraints, dbIndexes, dbIndexColumns]: ObjectLiteral[][] = await Promise.all([
            this.query(tablesSql),
            this.query(columnsSql),
            this.query(constraintsSql),
            this.query(indexesSql),
            this.query(indexeColumnsSql)
        ]);

        // if tables were not found in the db, no need to proceed
        if (!dbTables.length)
            return [];

        // create tables for loaded tables
        return dbTables.map(dbTable => {
            const table = new Table();
            table.name = this.driver.buildTableName(dbTable["TABLE_NAME"], currentSchema);

            // create columns from the loaded columns
            table.columns = dbColumns
                .filter(dbColumn => {
                    return dbColumn["TABLE_NAME"] === table.name || dbColumn["TABLE_NAME"] === dbTable["TABLE_NAME"];})
                .map(dbColumn => {
                    const columnConstraints = dbConstraints.filter(dbConstraint => dbConstraint["TABLE_NAME"] === table.name && dbConstraint["COLUMN_NAME"] === dbColumn["COLUMN_NAME"]);

                    const tableColumn = new TableColumn();
                    tableColumn.name = dbColumn["COLUMN_NAME"];
                    tableColumn.type = dbColumn["DATA_TYPE_NAME"].toLowerCase();
                    if (tableColumn.type.indexOf("(") !== -1)
                        tableColumn.type = tableColumn.type.replace(/\([0-9]*\)/, "");

                    // TODO check only columns that have length property
                    if (this.driver.withLengthColumnTypes.indexOf(tableColumn.type as ColumnType) !== -1) {
                        const length = dbColumn["LENGTH"];
                        tableColumn.length = length && !this.isDefaultColumnLength(table, tableColumn, length) ? length.toString() : "";
                    }
                    /* 
                                        if (tableColumn.type === "number" || tableColumn.type === "float") {
                                            if (dbColumn["DATA_PRECISION"] !== null && !this.isDefaultColumnPrecision(table, tableColumn, dbColumn["DATA_PRECISION"]))
                                                tableColumn.precision = dbColumn["DATA_PRECISION"];
                                            if (dbColumn["DATA_SCALE"] !== null && !this.isDefaultColumnScale(table, tableColumn, dbColumn["DATA_SCALE"]))
                                                tableColumn.scale = dbColumn["DATA_SCALE"];
                    
                                        } else if ((tableColumn.type === "timestamp"
                                            || tableColumn.type === "timestamp with time zone"
                                            || tableColumn.type === "timestamp with local time zone") && dbColumn["DATA_SCALE"] !== null) {
                                            tableColumn.precision = !this.isDefaultColumnPrecision(table, tableColumn, dbColumn["DATA_SCALE"]) ? dbColumn["DATA_SCALE"] : undefined;
                                        } */

                    // TODO
                    tableColumn.default = dbColumn["DEFAULT_VALUE"] !== null
                        && dbColumn["DEFAULT_VALUE"] !== undefined
                        && dbColumn["DEFAULT_VALUE"].trim() !== "NULL" ? tableColumn.default = dbColumn["DEFAULT_VALUE"].trim() : undefined;

                    tableColumn.isNullable = dbColumn["IS_NULLABLE"] === "TRUE";
                    tableColumn.isUnique = columnConstraints.length > 0 && columnConstraints[0]["IS_UNIQUE_KEY"] === "TRUE";
                    tableColumn.isPrimary = columnConstraints.length > 0 && columnConstraints[0]["IS_PRIMARY_KEY"] === "TRUE";
                    tableColumn.isGenerated = dbColumn["GENERATED_ALWAYS_AS"] !== null;
                    tableColumn.comment = ""; // todo
                    if (tableColumn.isGenerated) { // todo generationStrategy === "increment"
                        tableColumn.sequenceName = "_SYS_SEQUENCE_" + dbColumn["COLUMN_ID"] + "_#0_#";
                    }
                    return tableColumn;
                });

            // find check constraints of table, group them by constraint name and build TableCheck.
            table.checks = dbConstraints
                .filter(dbConstraint => (dbConstraint["TABLE_NAME"] === table.name || dbConstraint["TABLE_NAME"] === dbTable["TABLE_NAME"]) && dbConstraint["CHECK_CONDITION"] !== null)
                .map(constraint => {
                return new TableCheck({
                    name: constraint["CONSTRAINT_NAME"],
                    columnNames: [],
                    expression: constraint["CHECK_CONDITION"]
                });
            });

            // create TableIndex objects from the loaded indices
            table.indices = dbIndexes
                .filter(dbIndex => (dbIndex["TABLE_NAME"] === table.name || dbIndex["TABLE_NAME"] === dbTable["TABLE_NAME"]) && dbIndex["CONSTRAINT"] !== "PRIMARY KEY")
                .map(dbIndex => {
                    const tableIndex = new TableIndex({
                        name: dbIndex["INDEX_NAME"],
                        columnNames: dbIndexColumns.filter(dbIndexColumn => dbIndexColumn["INDEX_OID"] === dbIndex["INDEX_OID"]).map(dbIndexColumn => dbIndexColumn["COLUMN_NAME"]),
                        isUnique: dbIndex["INDEX_TYPE"] ? (dbIndex["INDEX_TYPE"]).includes("UNIQUE") : false
                    });

                    return tableIndex;
                });

            return table;
        });
    }

    /**
     * Builds create table sql
     */
    protected createTableSql(table: Table, createForeignKeys?: boolean): Query {

        const columnDefinitions = table.columns.map(column => this.buildCreateColumnSql(column)).join(", ");
        let sql = `CREATE COLUMN TABLE ${this.escapePath(table)} (${columnDefinitions}`;
        //  TODO constraints, refrences, etc.
        sql += `)`;
        return new Query(sql);
    }

    /**
     * Escapes given table or view path.
     */
    protected escapePath(target: Table|View|string, disableEscape?: boolean): string {
        let tableName = target instanceof Table || target instanceof View ? target.name : target;
        tableName = tableName.indexOf(".") === -1 && this.driver.options.schema ? `${this.driver.options.schema}.${tableName}` : tableName;

        return tableName.split(".").map(i => {
            return disableEscape ? i : `"${i}"`;
        }).join(".");
    }

     /**
     * Builds a query for create column.
     */
    protected buildCreateColumnSql(column: TableColumn) {
        let c = `"${column.name}" ` + this.connection.driver.createFullType(column);
        if (column.default !== undefined && column.default !== null) // DEFAULT must be placed before NOT NULL
            c += " DEFAULT " + column.default;
        if (column.isNullable !== true && !column.isGenerated) // NOT NULL is not supported with GENERATED
            c += " NOT NULL";
        if (column.isGenerated === true && column.generationStrategy === "increment")
            c += " GENERATED ALWAYS AS IDENTITY";

        return c;
    }


    /**
     * Builds drop table sql
     */
    protected dropTableSql(tableOrName: Table | string): Query {
        const tableName = tableOrName instanceof Table ? tableOrName.name : `\"${tableOrName}\"`;
        const query = `DROP TABLE ${tableName}`;
        return new Query(query);
    }

    protected createViewSql(view: View): Query {
        throw new OperationNotSupportedError();
    }

    protected async insertViewDefinitionSql(view: View): Promise<Query> {
        throw new OperationNotSupportedError();
    }

    /**
     * Builds drop view sql.
     */
    protected dropViewSql(viewOrPath: View | string): Query {
        throw new OperationNotSupportedError();
    }

    /**
     * Builds remove view sql.
     */
    protected async deleteViewDefinitionSql(viewOrPath: View | string): Promise<Query> {
        throw new OperationNotSupportedError();
    }

    /**
     * Builds create index sql.
     */
    protected createIndexSql(table: Table, index: TableIndex): Query {
        const columns = index.columnNames.map(columnName => `"${columnName}"`).join(", ");
        return new Query(`CREATE ${index.isUnique ? "UNIQUE " : ""}INDEX "${index.name}" ON ${this.escapePath(table)} (${columns})`);
    }

    /**
     * Builds drop index sql.
     */
    protected dropIndexSql(indexOrName: TableIndex|string): Query {
        let indexName = indexOrName instanceof TableIndex ? indexOrName.name : indexOrName;
        return new Query(`DROP INDEX "${indexName}"`);
    }

    /**
     * Builds create primary key sql.
     */
    protected createPrimaryKeySql(table: Table, columnNames: string[]): Query {
        const primaryKeyName = this.connection.namingStrategy.primaryKeyName(table.name, columnNames);
        const columnNamesString = columnNames.map(columnName => `"${columnName}"`).join(", ");
        return new Query(`ALTER TABLE ${this.escapePath(table)} ADD CONSTRAINT "${primaryKeyName}" PRIMARY KEY (${columnNamesString})`);
    }

    /**
     * Builds drop primary key sql.
     */
    protected dropPrimaryKeySql(table: Table): Query {
        const columnNames = table.primaryColumns.map(column => column.name);
        const primaryKeyName = this.connection.namingStrategy.primaryKeyName(table.name, columnNames);
        return new Query(`ALTER TABLE ${this.escapePath(table)} DROP CONSTRAINT "${primaryKeyName}"`);
    }

    /**
     * Builds create foreign key sql.
     */
    protected createForeignKeySql(table: Table, foreignKey: TableForeignKey): Query {
        throw new OperationNotSupportedError();
    }

    /**
     * Builds drop foreign key sql.
     */
    protected dropForeignKeySql(table: Table, foreignKeyOrName: TableForeignKey | string): Query {
        throw new OperationNotSupportedError();
    }

    protected parseTableName(target: Table | string) {
        throw new OperationNotSupportedError();
    }

    protected async getCurrentSchema(): Promise<string> {
        const currentSchemaQuery = await this.query(`SELECT CURRENT_SCHEMA FROM DUMMY`);
        return currentSchemaQuery[0]["CURRENT_SCHEMA"];
    }
}
