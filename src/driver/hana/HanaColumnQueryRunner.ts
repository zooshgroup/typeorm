import { QueryRunner } from "../../query-runner/QueryRunner";
import { TransactionAlreadyStartedError } from "../../error/TransactionAlreadyStartedError";
import { TransactionNotStartedError } from "../../error/TransactionNotStartedError";
import { TableColumn } from "../../schema-builder/table/TableColumn";
import { Table } from "../../schema-builder/table/Table";
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey";
import { TableIndex } from "../../schema-builder/table/TableIndex";
import { QueryRunnerAlreadyReleasedError } from "../../error/QueryRunnerAlreadyReleasedError";
import { View } from "../../schema-builder/view/View";
import { Sequence } from "../../schema-builder/sequence/Sequence";
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
import { OrmUtils } from "../../util/OrmUtils";
import { PromiseUtils } from '../../util/PromiseUtils';


/**
 * Runs queries on a hana database connection pool.
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

    protected databaseConnection: any;

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
        if (!this.databaseConnection) {
            this.databaseConnection = this.driver.pool.acquire();
        }

        return this.databaseConnection;
    }

    /**
     * Releases used database connection.
     * We just clear loaded tables and sql in memory, because sqlite do not support multiple connections thus query runners.
     */
    release(): Promise<void> {
        return new Promise<void>((ok, fail) => {
            this.isReleased = true;
            if (this.databaseConnection) {
                this.databaseConnection
                    .then((resource: any) => {
                        resource.rollback();
                        resource.setAutoCommit(true);
                        this.driver.pool.release(resource)
                        ok();
                    })
                    .catch(fail);
            } else {
                ok();
            }
        });
    }

    /**
     * Starts transaction on the current connection.
     */
    async startTransaction(isolationLevel?: IsolationLevel): Promise<void> {
        if (this.isTransactionActive)
            throw new TransactionAlreadyStartedError();

        let connection = await this.connect();
        connection.setAutoCommit(false);

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

        const connection = await this.databaseConnection
        connection.commit();
        connection.setAutoCommit(true);

        this.isTransactionActive = false;
    }

    /**
     * Rollbacks transaction.
     * Error will be thrown if transaction was not started.
     */
    async rollbackTransaction(): Promise<void> {
        if (!this.isTransactionActive)
            throw new TransactionNotStartedError();

        const connection = await this.databaseConnection;
        connection.rollback();
        connection.setAutoCommit(true);
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
        return Promise.resolve([]);
    }

    /**
     * Checks if database with the given name exist.
     */
    async hasDatabase(database: string): Promise<boolean> {
        return Promise.resolve(false);
    }

    /**
     * Checks if schema with the given name exist.
     */
    async hasSchema(schema: string): Promise<boolean> {
        const result = await this.query(`SELECT * FROM "SCHEMAS" WHERE "SCHEMA_NAME" = '${schema}'`);
        return result.length ? true : false;
    }

    /**
     * Checks if table with the given name exist in the database.
     */
    async hasTable(tableOrName: Table | string): Promise<boolean> {
        const parsedTableName = this.parseTableViewName(tableOrName);
        const sql = `SELECT "TABLE_NAME" FROM "TABLES" WHERE "TABLE_NAME" = '${parsedTableName.name}' AND SCHEMA_NAME = '${parsedTableName.schema}'`;
        const result = await this.query(sql);
        return result.length ? true : false;
    }

    /**
     * Checks if column with the given name exist in the given table.
     */
    async hasColumn(tableOrName: Table | string, columnName: string): Promise<boolean> {
        const parsedTableName = this.parseTableViewName(tableOrName);
        const sql = `SELECT * FROM "TABLE_COLUMNS" WHERE "SCHEMA_NAME" = ${parsedTableName.schema} AND "TABLE_NAME" = ${parsedTableName.name} AND "COLUMN_NAME" = '${columnName}'`;
        const result = await this.query(sql);
        return result.length ? true : false;
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
    async dropTable(tableOrName: Table | string, ifExist?: boolean, dropForeignKeys: boolean = true, dropIndices: boolean = true): Promise<void> {// It needs because if table does not exist and dropForeignKeys or dropIndices is true, we don't need
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
        const upQueries: Query[] = [];
        const downQueries: Query[] = [];
        upQueries.push(this.createViewSql(view));
        upQueries.push(this.insertViewDefinitionSql(view));
        downQueries.push(this.dropViewSql(view));
        downQueries.push(this.deleteViewDefinitionSql(view));
        await this.executeQueries(upQueries, downQueries);
    }

    /**
     * Drops the view.
     */
    async dropView(target: View|string): Promise<void> {
        const viewName = target instanceof View ? target.name : target;
        const view = await this.getCachedView(viewName);

        const upQueries: Query[] = [];
        const downQueries: Query[] = [];
        upQueries.push(this.deleteViewDefinitionSql(view));
        upQueries.push(this.dropViewSql(view));
        downQueries.push(this.insertViewDefinitionSql(view));
        downQueries.push(this.createViewSql(view));
        await this.executeQueries(upQueries, downQueries);
    }

    /**
    * Creates a new sequence.
    */
    async createSequence(sequence: Sequence): Promise<void> {
        const upQueries: Query[] = [];
        const downQueries: Query[] = [];

        upQueries.push(this.createSequenceSql(sequence));
        downQueries.push(this.dropSequenceSql(sequence));

        await this.executeQueries(upQueries, downQueries);
    }

    /**
     * Drops the sequence.
     */
    async dropSequence(target: Sequence | string): Promise<void> {
        const upQueries: Query[] = [];
        const downQueries: Query[] = [];

        //const sequence = target instanceof Sequence ? target : await this.getCachedSequence(target);

        upQueries.push(this.dropSequenceSql(target instanceof Sequence ? target.name : target));
        //downQueries.push(this.createSequenceSql(sequence));

        await this.executeQueries(upQueries, downQueries);
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
    async addColumn(tableOrName: Table|string, column: TableColumn): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const clonedTable = table.clone();
        const upQueries: Query[] = [];
        const downQueries: Query[] = [];

        upQueries.push(new Query(`ALTER TABLE ${this.escapePath(table.name)} ADD (${this.buildCreateColumnSql(column)})`));
        downQueries.push(new Query(`ALTER TABLE ${this.escapePath(table.name)} DROP ("${column.name}")`));

        // create or update primary key constraint
        if (column.isPrimary) {
            const primaryColumns = clonedTable.primaryColumns;
            // if table already have primary key, me must drop it and recreate again
            if (primaryColumns.length > 0) {
                const pkName = this.connection.namingStrategy.primaryKeyName(clonedTable.name, primaryColumns.map(column => column.name));
                const columnNames = primaryColumns.map(column => `"${column.name}"`).join(", ");
                upQueries.push(new Query(`ALTER TABLE ${this.escapePath(table.name)} DROP CONSTRAINT "${pkName}"`));
                downQueries.push(new Query(`ALTER TABLE ${this.escapePath(table.name)} ADD CONSTRAINT "${pkName}" PRIMARY KEY (${columnNames})`));
            }

            primaryColumns.push(column);
            const pkName = this.connection.namingStrategy.primaryKeyName(clonedTable.name, primaryColumns.map(column => column.name));
            const columnNames = primaryColumns.map(column => `"${column.name}"`).join(", ");
            upQueries.push(new Query(`ALTER TABLE ${this.escapePath(table.name)} ADD CONSTRAINT "${pkName}" PRIMARY KEY (${columnNames})`));
            downQueries.push(new Query(`ALTER TABLE ${this.escapePath(table.name)} DROP CONSTRAINT "${pkName}"`));
        }

        // create column index
        const columnIndex = clonedTable.indices.find(index => index.columnNames.length === 1 && index.columnNames[0] === column.name);
        if (columnIndex) {
            clonedTable.indices.splice(clonedTable.indices.indexOf(columnIndex), 1);
            upQueries.push(this.createIndexSql(table, columnIndex));
            downQueries.push(this.dropIndexSql(columnIndex));
        }

        // create unique constraint
        if (column.isUnique) {
            const uniqueConstraint = new TableUnique({
                name: this.connection.namingStrategy.uniqueConstraintName(table.name, [column.name]),
                columnNames: [column.name]
            });
            clonedTable.uniques.push(uniqueConstraint);
            upQueries.push(new Query(`ALTER TABLE ${this.escapePath(table.name)} ADD CONSTRAINT "${uniqueConstraint.name}" UNIQUE ("${column.name}")`));
            downQueries.push(new Query(`ALTER TABLE ${this.escapePath(table.name)} DROP CONSTRAINT "${uniqueConstraint.name}"`));
        }

        await this.executeQueries(upQueries, downQueries);

        clonedTable.addColumn(column);
        this.replaceCachedTable(table, clonedTable);
    }

    /**
     * Creates a new columns from the column in the table.
     */
    async addColumns(tableOrName: Table | string, columns: TableColumn[]): Promise<void> {
        await PromiseUtils.runInSequence(columns, column => this.addColumn(tableOrName, column));
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
    async changeColumn(tableOrName: Table | string, oldTableColumnOrName: TableColumn | string, newColumn: TableColumn): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Extracts schema name from given Table object or table name string.
     */
    protected extractSchema(target: Table | string): string | undefined {
        const tableName = target instanceof Table ? target.name : target;
        return tableName.indexOf(".") === -1 ? this.driver.options.schema : tableName.split(".")[0];
    }

    /**
     * Changes a column in the table.
     */
    async changeColumns(tableOrName: Table | string, changedColumns: { newColumn: TableColumn, oldColumn: TableColumn }[]): Promise<void> {
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
    async updatePrimaryKeys(tableOrName: Table|string, columns: TableColumn[]): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const clonedTable = table.clone();
        const columnNames = columns.map(column => column.name);
        const upQueries: Query[] = [];
        const downQueries: Query[] = [];

        // if table already have primary columns, we must drop them.
        const primaryColumns = clonedTable.primaryColumns;
        if (primaryColumns.length > 0) {
            const pkName = this.connection.namingStrategy.primaryKeyName(clonedTable.name, primaryColumns.map(column => column.name));
            const columnNamesString = primaryColumns.map(column => `"${column.name}"`).join(", ");
            upQueries.push(new Query(`ALTER TABLE ${this.escapePath(table)} DROP CONSTRAINT "${pkName}"`));
            downQueries.push(new Query(`ALTER TABLE ${this.escapePath(table)} ADD CONSTRAINT "${pkName}" PRIMARY KEY (${columnNamesString})`));
        }

        // update columns in table.
        clonedTable.columns
            .filter(column => columnNames.indexOf(column.name) !== -1)
            .forEach(column => column.isPrimary = true);

        const pkName = this.connection.namingStrategy.primaryKeyName(clonedTable.name, columnNames);
        const columnNamesString = columnNames.map(columnName => `"${columnName}"`).join(", ");
        upQueries.push(new Query(`ALTER TABLE ${this.escapePath(table)} ADD CONSTRAINT "${pkName}" PRIMARY KEY (${columnNamesString})`));
        downQueries.push(new Query(`ALTER TABLE ${this.escapePath(table)} DROP CONSTRAINT "${pkName}"`));

        await this.executeQueries(upQueries, downQueries);
        this.replaceCachedTable(table, clonedTable);
    }

    /**
     * Drops a primary key.
     */
    async dropPrimaryKey(tableOrName: Table|string): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const up = this.dropPrimaryKeySql(table);
        const down = this.createPrimaryKeySql(table, table.primaryColumns.map(column => column.name));
        await this.executeQueries(up, down);
        table.primaryColumns.forEach(column => {
            column.isPrimary = false;
        });
    }

    /**
      * Creates a new unique constraint.
      */
    async createUniqueConstraint(tableOrName: Table | string, uniqueConstraint: TableUnique): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);

        // new unique constraint may be passed without name. In this case we generate unique name manually.
        if (!uniqueConstraint.name)
            uniqueConstraint.name = this.connection.namingStrategy.uniqueConstraintName(table.name, uniqueConstraint.columnNames);

        const up = this.createUniqueConstraintSql(table, uniqueConstraint);
        const down = this.dropUniqueConstraintSql(table, uniqueConstraint);
        await this.executeQueries(up, down);
        table.addUniqueConstraint(uniqueConstraint);
    }

    /**
     * Builds create unique constraint sql.
     */
    protected createUniqueConstraintSql(table: Table, uniqueConstraint: TableUnique): Query {
        const columnNames = uniqueConstraint.columnNames.map(column => `"` + column + `"`).join(", ");
        return new Query(`ALTER TABLE ${this.escapePath(table.name)} ADD CONSTRAINT "${uniqueConstraint.name}" UNIQUE (${columnNames})`);
    }

    /**
     * Builds drop unique constraint sql.
     */
    protected dropUniqueConstraintSql(table: Table, uniqueOrName: TableUnique | string): Query {
        const uniqueName = uniqueOrName instanceof TableUnique ? uniqueOrName.name : uniqueOrName;
        return new Query(`ALTER TABLE ${this.escapePath(table.name)} DROP CONSTRAINT "${uniqueName}"`);
    }

    /**
     * Creates a new unique constraints.
     */
    async createUniqueConstraints(tableOrName: Table | string, uniqueConstraints: TableUnique[]): Promise<void> {
        const promises = uniqueConstraints.map(uniqueConstraint => this.createUniqueConstraint(tableOrName, uniqueConstraint));
        await Promise.all(promises);
    }

    /**
     * Drops an unique constraint.
     */
    async dropUniqueConstraint(tableOrName: Table | string, uniqueOrName: TableUnique | string): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const uniqueConstraint = uniqueOrName instanceof TableUnique ? uniqueOrName : table.uniques.find(u => u.name === uniqueOrName);
        if (!uniqueConstraint)
            throw new Error(`Supplied unique constraint was not found in table ${table.name}`);

        const up = this.dropUniqueConstraintSql(table, uniqueConstraint);
        const down = this.createUniqueConstraintSql(table, uniqueConstraint);
        await this.executeQueries(up, down);
        table.removeUniqueConstraint(uniqueConstraint);
    }

    /**
     * Creates an unique constraints.
     */
    async dropUniqueConstraints(tableOrName: Table | string, uniqueConstraints: TableUnique[]): Promise<void> {
        const promises = uniqueConstraints.map(uniqueConstraint => this.dropUniqueConstraint(tableOrName, uniqueConstraint));
        await Promise.all(promises);
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
    protected dropCheckConstraintSql(table: Table, checkOrName: TableCheck | string): Query {
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
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const checkConstraint = checkOrName instanceof TableCheck ? checkOrName : table.checks.find(c => c.name === checkOrName);
        if (!checkConstraint)
            throw new Error(`Supplied check constraint was not found in table ${table.name}`);

        const up = this.dropCheckConstraintSql(table, checkConstraint);
        const down = this.createCheckConstraintSql(table, checkConstraint);
        await this.executeQueries(up, down);
        table.removeCheckConstraint(checkConstraint);
    }

    /**
     * Drops check constraints.
     */
    async dropCheckConstraints(tableOrName: Table | string, checkConstraints: TableCheck[]): Promise<void> {
        const promises = checkConstraints.map(checkConstraint => this.dropCheckConstraint(tableOrName, checkConstraint));
        await Promise.all(promises);
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
    async createForeignKey(tableOrName: Table|string, foreignKey: TableForeignKey): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);

        // new FK may be passed without name. In this case we generate FK name manually.
        if (!foreignKey.name)
            foreignKey.name = this.connection.namingStrategy.foreignKeyName(table.name, foreignKey.columnNames);

        const up = this.createForeignKeySql(table, foreignKey);
        const down = this.dropForeignKeySql(table, foreignKey);
        await this.executeQueries(up, down);
        table.addForeignKey(foreignKey);
    }

    /**
     * Creates a new foreign keys.
     */
    async createForeignKeys(tableOrName: Table|string, foreignKeys: TableForeignKey[]): Promise<void> {
        const promises = foreignKeys.map(foreignKey => this.createForeignKey(tableOrName, foreignKey));
        await Promise.all(promises);
    }

    /**
     * Drops a foreign key from the table.
     */
    async dropForeignKey(tableOrName: Table|string, foreignKeyOrName: TableForeignKey|string): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const foreignKey = foreignKeyOrName instanceof TableForeignKey ? foreignKeyOrName : table.foreignKeys.find(fk => fk.name === foreignKeyOrName);
        if (!foreignKey)
            throw new Error(`Supplied foreign key was not found in table ${table.name}`);

        const up = this.dropForeignKeySql(table, foreignKey);
        const down = this.createForeignKeySql(table, foreignKey);
        await this.executeQueries(up, down);
        table.removeForeignKey(foreignKey);
    }

    /**
     * Drops a foreign keys from the table.
     */
    async dropForeignKeys(tableOrName: Table|string, foreignKeys: TableForeignKey[]): Promise<void> {
        const promises = foreignKeys.map(foreignKey => this.dropForeignKey(tableOrName, foreignKey));
        await Promise.all(promises);
    }

    /**
     * Creates a new index.
     */
    async createIndex(tableOrName: Table | string, index: TableIndex): Promise<void> {
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
    async createIndices(tableOrName: Table | string, indices: TableIndex[]): Promise<void> {
        const promises = indices.map(index => this.createIndex(tableOrName, index));
        await Promise.all(promises);
    }

    async dropIndex(tableOrName: Table | string, indexOrName: TableIndex | string): Promise<void> {
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
    async dropIndices(tableOrName: Table | string, indices: TableIndex[]): Promise<void> {
        const promises = indices.map(index => this.dropIndex(tableOrName, index));
        await Promise.all(promises);
    }

    /**
     * Clears all table contents.
     * Note: this operation uses SQL's TRUNCATE query which cannot be reverted in transactions.
     */
    async clearTable(tableOrName: Table | string): Promise<void> {
        await this.query(`TRUNCATE TABLE ${this.escapePath(tableOrName)}`);
    }

    /**
     * Removes all tables from the currently connected database.
     * Be careful using this method and avoid using it in production or migrations
     * (because it can clear all your database).
     */
    async clearDatabase(database?: string): Promise<void> {
        const currentSchema = await this.getCurrentSchema();

        try {
            const dropViewsQuery = `SELECT 'DROP VIEW "' || SCHEMA_NAME || '"."' || VIEW_NAME || '"' AS "query" FROM "VIEWS" WHERE SCHEMA_NAME = '${currentSchema}'`;
            const dropViewQueries: ObjectLiteral[] = await this.query(dropViewsQuery);
            await Promise.all(dropViewQueries.map(query => this.query(query["query"])));

            const dropTablesQuery = `SELECT 'DROP TABLE "' || SCHEMA_NAME || '"."' || TABLE_NAME || '" CASCADE' AS "query" FROM "TABLES" WHERE SCHEMA_NAME = '${currentSchema}'`;
            const dropTableQueries: ObjectLiteral[] = await this.query(dropTablesQuery);
            await Promise.all(dropTableQueries.map(query => this.query(query["query"])));

            const dropSequencesQuery = `SELECT 'DROP SEQUENCE "' || SCHEMA_NAME || '"."' || SEQUENCE_NAME || '"' AS "query" FROM "SEQUENCES" WHERE SCHEMA_NAME = '${currentSchema}'`;
            const dropSequencesQueries: ObjectLiteral[] = await this.query(dropSequencesQuery);
            await Promise.all(dropSequencesQueries.map(query => this.query(query["query"])));
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
        const hasTable = await this.hasTable(this.getTypeormMetadataTableName());
        if (!hasTable)
            return Promise.resolve([]);

        const currentSchema = await this.getCurrentSchema();
        const viewsCondition = viewNames.map(viewName => {
            let [schema, name] = viewName.split(".");
            if (!name) {
                name = schema;
                schema = this.driver.options.schema || currentSchema;
            }
            return `("t"."schema" = '${schema}' AND "t"."name" = '${name}')`;
        }).join(" OR ");
 
        const query = `SELECT "t".* FROM ${this.escapePath(this.getTypeormMetadataTableName())} "t" ` +
            `INNER JOIN "VIEWS" "v" ON "v"."SCHEMA_NAME" = "t"."schema" AND "v"."VIEW_NAME" = "t"."name" WHERE "t"."type" = 'VIEW' ${viewsCondition ? `AND (${viewsCondition})` : ""}`;
        const dbViews = await this.query(query);
        return dbViews.map((dbView: any) => {
            const view = new View();
            const schema = dbView["schema"] === currentSchema && !this.driver.options.schema ? undefined : dbView["schema"];
            view.name = this.driver.buildTableName(dbView["name"], schema);
            view.expression = dbView["value"];
            return view;
        });
    }

    protected async loadSequences(sequencePathes: string[]): Promise<Sequence[]> {
        if (!sequencePathes || !sequencePathes.length)
            return [];

        const currentSchema = await this.getCurrentSchema();

        const sequenceNamesString = sequencePathes.map(name => "'" + (name.startsWith(currentSchema + ".") ? name.substring(currentSchema.length + 1) : name) + "'").join(", ");

        const sequencesSql = `SELECT * FROM "SEQUENCES" WHERE "SEQUENCE_NAME" IN (${sequenceNamesString}) AND SCHEMA_NAME = '${currentSchema}'`;

        const dbSequences: ObjectLiteral[] = await this.query(sequencesSql);

        return dbSequences.map(dbSequence =>  new Sequence(dbSequence["SEQUENCE_NAME"]));
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
        const foreignKeysSql = `SELECT * FROM "REFERENTIAL_CONSTRAINTS" WHERE "TABLE_NAME" IN (${tableNamesString}) AND "SCHEMA_NAME" = '${currentSchema}'`;

        const [dbTables, dbColumns, dbConstraints, dbIndexes, dbIndexColumns, dbForeignKeys]: ObjectLiteral[][] = await Promise.all([
            this.query(tablesSql),
            this.query(columnsSql),
            this.query(constraintsSql),
            this.query(indexesSql),
            this.query(indexeColumnsSql),
            this.query(foreignKeysSql),
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
                    return dbColumn["TABLE_NAME"] === dbTable["TABLE_NAME"];
                })
                .map(dbColumn => {
                    const columnConstraints = dbConstraints.filter(dbConstraint => dbConstraint["TABLE_NAME"] === dbTable["TABLE_NAME"] && dbConstraint["COLUMN_NAME"] === dbColumn["COLUMN_NAME"]);

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
                    tableColumn.isGenerated = dbColumn["GENERATED_ALWAYS_AS"] !== null; // todo this will not be set
                    tableColumn.comment = ""; // todo
                    return tableColumn;
                });

            // find unique constraints of table, group them by constraint name and build TableUnique.
            const tableUniqueConstraints = OrmUtils.uniq(dbConstraints.filter(dbConstraint => {
                return dbConstraint["TABLE_NAME"] === dbTable["TABLE_NAME"] && dbConstraint["IS_UNIQUE_KEY"] === "TRUE" && dbConstraint["IS_PRIMARY_KEY"] !== "TRUE";
            }), dbConstraint => dbConstraint["CONSTRAINT_NAME"]);

            table.uniques = tableUniqueConstraints.map(constraint => {
                const uniques = dbConstraints.filter(dbC => dbC["CONSTRAINT_NAME"] === constraint["CONSTRAINT_NAME"]);
                return new TableUnique({
                    name: constraint["CONSTRAINT_NAME"],
                    columnNames: uniques.map(u => u["COLUMN_NAME"])
                });
            });

            // find check constraints of table, group them by constraint name and build TableCheck.
            table.checks = dbConstraints
                .filter(dbConstraint => dbConstraint["TABLE_NAME"] === dbTable["TABLE_NAME"] && dbConstraint["CHECK_CONDITION"] !== null)
                .map(constraint => {
                    return new TableCheck({
                        name: constraint["CONSTRAINT_NAME"],
                        columnNames: [],
                        expression: constraint["CHECK_CONDITION"]
                    });
                });

            // find foreign key constraints of table, group them by constraint name and build TableForeignKey.
             const tableForeignKeyConstraints = OrmUtils.uniq(dbForeignKeys.filter(dbForeignKey => {
                return dbForeignKey["TABLE_NAME"] === dbTable["TABLE_NAME"];
            }), dbForeignKey => dbForeignKey["CONSTRAINT_NAME"]);

            table.foreignKeys = tableForeignKeyConstraints.map(dbForeignKey => {
                const foreignKeys = dbForeignKeys.filter(dbFk => dbFk["CONSTRAINT_NAME"] === dbForeignKey["CONSTRAINT_NAME"]);
                return new TableForeignKey({
                    name: dbForeignKey["CONSTRAINT_NAME"],
                    columnNames: foreignKeys.map(dbFk => dbFk["COLUMN_NAME"]),
                    referencedTableName: dbForeignKey["REFERENCED_TABLE_NAME"],
                    referencedColumnNames: foreignKeys.map(dbFk => dbFk["REFERENCED_COLUMN_NAME"]),
                    onDelete: dbForeignKey["DELETE_RULE"],
                    onUpdate: dbForeignKey["UPDATE_RULE"]
                });
            });

            // create TableIndex objects from the loaded indices
            table.indices = dbIndexes
                .filter(dbIndex => dbIndex["TABLE_NAME"] === dbTable["TABLE_NAME"] && dbIndex["CONSTRAINT"] !== "PRIMARY KEY")
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

        table.columns
            .filter(column => column.isUnique)
            .forEach(column => {
                const isUniqueExist = table.uniques.some(unique => unique.columnNames.length === 1 && unique.columnNames[0] === column.name);
                if (!isUniqueExist)
                    table.uniques.push(new TableUnique({
                        name: this.connection.namingStrategy.uniqueConstraintName(table.name, [column.name]),
                        columnNames: [column.name]
                    }));
            });

        if (table.uniques.length > 0) {
            const uniquesSql = table.uniques.map(unique => {
                const uniqueName = unique.name ? unique.name : this.connection.namingStrategy.uniqueConstraintName(table.name, unique.columnNames);
                const columnNames = unique.columnNames.map(columnName => `"${columnName}"`).join(", ");
                return `CONSTRAINT "${uniqueName}" UNIQUE (${columnNames})`;
            }).join(", ");

            sql += `, ${uniquesSql}`;
        }

        if (table.checks.length > 0) {
            const checksSql = table.checks.map(check => {
                const checkName = check.name ? check.name : this.connection.namingStrategy.checkConstraintName(table.name, check.expression!);
                return `CONSTRAINT "${checkName}" CHECK (${check.expression})`;
            }).join(", ");

            sql += `, ${checksSql}`;
        }

        if (table.foreignKeys.length > 0 && createForeignKeys) {
            const foreignKeysSql = table.foreignKeys.map(fk => {
                const columnNames = fk.columnNames.map(columnName => `"${columnName}"`).join(", ");
                if (!fk.name)
                    fk.name = this.connection.namingStrategy.foreignKeyName(table.name, fk.columnNames);
                const referencedColumnNames = fk.referencedColumnNames.map(columnName => `"${columnName}"`).join(", ");
                let constraint = `CONSTRAINT "${fk.name}" FOREIGN KEY (${columnNames}) REFERENCES ${this.escapePath(fk.referencedTableName)} (${referencedColumnNames})`;
                if (fk.onDelete && fk.onDelete !== "NO ACTION") // Oracle does not support NO ACTION, but we set NO ACTION by default in EntityMetadata
                    constraint += ` ON DELETE ${fk.onDelete}`;

                return constraint;
            }).join(", ");

            sql += `, ${foreignKeysSql}`;
        }

        const primaryColumns = table.columns.filter(column => column.isPrimary);
        if (primaryColumns.length > 0) {
            const primaryKeyName = this.connection.namingStrategy.primaryKeyName(table.name, primaryColumns.map(column => column.name));
            const columnNames = primaryColumns.map(column => `"${column.name}"`).join(", ");
            sql += `, CONSTRAINT "${primaryKeyName}" PRIMARY KEY (${columnNames})`;
        }

        sql += `)`;
        return new Query(sql);
    }

    /**
     * Escapes given table or view path.
     */
    protected escapePath(target: Table | View | string, disableEscape?: boolean): string {
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

        return c;
    }


    /**
     * Builds drop table sql
     */
    protected dropTableSql(tableOrName: Table | string): Query {
        const query = `DROP TABLE ${this.escapePath(tableOrName)}`;
        return new Query(query);
    }

    protected createSequenceSql(sequence: Sequence): Query {
        const query = `CREATE SEQUENCE "${sequence.name}"`;
        return new Query(query);
    }

    protected dropSequenceSql(sequenceOrName: Sequence | string): Query {
        const sequenceName = sequenceOrName instanceof Sequence ? sequenceOrName.name : `\"${sequenceOrName}\"`;
        const query = `DROP SEQUENCE "${sequenceName}"`;
        return new Query(query);
    }

    protected createViewSql(view: View): Query {
        if (typeof view.expression === "string") {
            return new Query(`CREATE VIEW ${this.escapePath(view)} AS ${view.expression}`);
        } else {
            return new Query(`CREATE VIEW ${this.escapePath(view)} AS ${view.expression(this.connection).getQuery()}`);
        }
    }

    protected insertViewDefinitionSql(view: View): Query {
        const parsedViewName = this.parseTableViewName(view);
        const expression = typeof view.expression === "string" ? view.expression.trim() : view.expression(this.connection).getQuery();
        const [query, parameters] = this.connection.createQueryBuilder()
            .insert()
            .into(this.getTypeormMetadataTableName())
            .values({ type: "VIEW", schema: parsedViewName.schema, name: parsedViewName.name, value: expression })
            .getQueryAndParameters();

        return new Query(query, parameters);
    }

    /**
     * Builds drop view sql.
     */
    protected dropViewSql(viewOrPath: View|string): Query {
        return new Query(`DROP VIEW ${this.escapePath(viewOrPath)}`);
    }

    /**
     * Builds remove view sql.
     */
    protected deleteViewDefinitionSql(viewOrPath: View|string): Query {
        const parsedViewName = this.parseTableViewName(viewOrPath);
        const qb = this.connection.createQueryBuilder();
        const [query, parameters] = qb.delete()
            .from(this.getTypeormMetadataTableName())
            .where(`${qb.escape("type")} = 'VIEW'`)
            .andWhere(`${qb.escape("name")} = :name`, { name: parsedViewName.name })
            .andWhere(`${qb.escape("schema")} = :schema`, { name: parsedViewName.schema })
            .getQueryAndParameters();

        return new Query(query, parameters);
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
    protected dropIndexSql(indexOrName: TableIndex | string): Query {
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
        const columnNames = foreignKey.columnNames.map(column => `"` + column + `"`).join(", ");
        const referencedColumnNames = foreignKey.referencedColumnNames.map(column => `"` + column + `"`).join(",");
        let sql = `ALTER TABLE ${this.escapePath(table)} ADD CONSTRAINT "${foreignKey.name}" FOREIGN KEY (${columnNames}) ` +
            `REFERENCES ${this.escapePath(foreignKey.referencedTableName)} (${referencedColumnNames})`;
        // Oracle does not support NO ACTION, but we set NO ACTION by default in EntityMetadata
        if (foreignKey.onDelete && foreignKey.onDelete !== "NO ACTION")
            sql += ` ON DELETE ${foreignKey.onDelete}`;

        return new Query(sql);
    }

    /**
     * Builds drop foreign key sql.
     */
    protected dropForeignKeySql(table: Table, foreignKeyOrName: TableForeignKey|string): Query {
        const foreignKeyName = foreignKeyOrName instanceof TableForeignKey ? foreignKeyOrName.name : foreignKeyOrName;
        return new Query(`ALTER TABLE ${this.escapePath(table)} DROP CONSTRAINT "${foreignKeyName}"`);
    }

    /**
     * Returns object with table schema and table name.
     */
    protected parseTableViewName(target: Table|View|string) {
        const tableName = target instanceof Table || target instanceof View ? target.name : target;
        if (tableName.indexOf(".") === -1) {
            return {
                schema: this.driver.options.schema ? `${this.driver.options.schema}` : `${this.getCurrentSchema()}`,
                name: `${tableName}`
            };
        } else {
            return {
                schema: `${tableName.split(".")[0]}`,
                name: `${tableName.split(".")[1]}`
            };
        }
    }

    protected async getCurrentSchema(): Promise<string> {
        const currentSchemaQuery = await this.query(`SELECT CURRENT_SCHEMA FROM DUMMY`);
        return currentSchemaQuery[0]["CURRENT_SCHEMA"];
    }
}
